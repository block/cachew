package client

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"
	"golang.org/x/sys/unix"
)

// pendingDir is directory metadata that GNU tar applies once it stops extracting into the directory.
type pendingDir struct {
	// name is the member name; GNU tar compares names as strings to decide when a directory is finished.
	name string
	path string
	stat entryStat
	// interdir marks a missing parent created for an entry, which only gets its permission bits restored.
	interdir bool
	// created is the mode a directory was made with; GNU tar skips the chmod when it already matches.
	created    uint32
	wasCreated bool
	// afterLinks holds the directory back until delayed links exist; id detects it being replaced meanwhile.
	afterLinks bool
	id         fileID
}

// dirStack mirrors GNU tar's delayed_set_stat list, deferring the metadata calls it would make mid-extraction.
//
// GNU tar applies a directory's metadata when an entry outside it arrives. Applying it then would mean waiting for
// queued writes, so finished directories are recorded and only applied early if a later entry modifies one.
type dirStack struct {
	// pending is the list GNU tar keeps, innermost directory last.
	pending []*pendingDir
	byName  map[string]*pendingDir
	// finished holds directories GNU tar would already have applied, in order, keyed by path in finishedPaths.
	finished      []*pendingDir
	finishedPaths map[string]struct{}
}

func newDirStack() dirStack {
	return dirStack{byName: map[string]*pendingDir{}, finishedPaths: map[string]struct{}{}}
}

func (s *dirStack) push(dir pendingDir) error {
	// A name already pending is updated in place, keeping its position and whether it is held back.
	existing, ok := s.byName[dir.name]
	if ok {
		dir.afterLinks, dir.id = existing.afterLinks, existing.id
		if existing.interdir && existing.afterLinks {
			info, err := os.Lstat(dir.path)
			if err != nil {
				return errors.WithStack(err)
			}
			dir.id = idOf(info)
		}
		*existing = dir
	} else {
		existing = &dir
		s.pending = append(s.pending, existing)
		s.byName[dir.name] = existing
	}
	if mustBeDot(dir.name) {
		return s.markAfterLinks(len(s.pending) - 1)
	}
	return nil
}

func (s *dirStack) finishNonAncestors(name string) {
	// GNU tar applies these before extracting name; recording them lets queued writes keep running.
	for len(s.pending) > 0 {
		top := s.pending[len(s.pending)-1]
		if top.afterLinks || isNameAncestor(top.name, name) {
			return
		}
		s.pop()
		s.finished = append(s.finished, top)
		s.finishedPaths[top.path] = struct{}{}
	}
}

func (s *dirStack) pop() *pendingDir {
	top := s.pending[len(s.pending)-1]
	s.pending = s.pending[:len(s.pending)-1]
	delete(s.byName, top.name)
	return top
}

func (s *dirStack) touchesFinished(root, path string) bool {
	// Writing path changes its parent's mtime, so an applied directory on the way must be applied first.
	if len(s.finishedPaths) == 0 {
		return false
	}
	for ; len(path) >= len(root); path = filepath.Dir(path) {
		if _, ok := s.finishedPaths[path]; ok {
			return true
		}
		if path == root {
			return false
		}
	}
	return false
}

func (s *dirStack) markAfterLinks(i int) error {
	// Held-back directories wait for delayed links, so their outer directories must wait too.
	for ; i >= 0 && !s.pending[i].afterLinks; i-- {
		dir := s.pending[i]
		dir.afterLinks = true
		info, err := os.Lstat(dir.path)
		if err != nil {
			return errors.WithStack(err)
		}
		dir.id = idOf(info)
	}
	return nil
}

func (s *dirStack) markLinkParent(name string) error {
	// Only the directory directly containing a delayed link is held back, matching GNU tar's string comparison.
	for i := len(s.pending) - 1; i >= 0; i-- {
		dir := s.pending[i]
		if dir.afterLinks {
			continue
		}
		rest, ok := strings.CutPrefix(name, dir.name+"/")
		if ok && !strings.Contains(rest, "/") {
			return s.markAfterLinks(i)
		}
	}
	return nil
}

func isNameAncestor(dir, name string) bool {
	return len(dir) < len(name) && strings.HasPrefix(name, dir) &&
		(name[len(dir)] == '/' || strings.HasSuffix(dir, "/"))
}

func mustBeDot(name string) bool {
	for {
		rest, ok := strings.CutPrefix(name, "./")
		if !ok {
			return name == "" || name == "."
		}
		name = strings.TrimLeft(rest, "/")
	}
}

func (e *tarExtractor) flushFinished() error {
	// Queued writes into these directories must land first or they would bump the restored mtimes.
	e.waitAll()
	for _, dir := range e.dirStack.finished {
		if err := e.applyDir(dir, false); err != nil {
			return err
		}
	}
	e.dirStack.finished = nil
	clear(e.dirStack.finishedPaths)
	return nil
}

// finish applies directory metadata and delayed links in GNU tar's order once every entry is extracted.
func (e *tarExtractor) finish() error {
	if err := e.flushFinished(); err != nil {
		return err
	}
	stack := &e.dirStack
	for len(stack.pending) > 0 && !stack.pending[len(stack.pending)-1].afterLinks {
		if err := e.applyDir(stack.pop(), false); err != nil {
			return err
		}
	}
	if err := e.applyDelayedLinks(); err != nil {
		return err
	}
	verify := false
	for len(stack.pending) > 0 {
		dir := stack.pop()
		// From the first held-back directory on, GNU tar checks that each is still the same directory.
		verify = verify || dir.afterLinks
		if err := e.applyDir(dir, verify); err != nil {
			return err
		}
	}
	return nil
}

func (e *tarExtractor) applyDir(dir *pendingDir, verify bool) error {
	if verify {
		info, err := os.Lstat(dir.path)
		if err != nil {
			return errors.WithStack(err)
		}
		if idOf(info) != dir.id {
			return errors.Errorf("%s: directory renamed before its status could be extracted", dir.path)
		}
	}
	if dir.interdir {
		var st unix.Stat_t
		if err := unix.Lstat(dir.path, &st); err != nil {
			return errors.Wrapf(err, "failed to stat %s", dir.path)
		}
		// Only permission bits are restored, keeping any set-group-ID bit inherited from the parent.
		mode := st.Mode&0o7000 | dir.stat.mode
		if mode == st.Mode&0o7777 {
			return nil
		}
		return errors.Wrapf(unix.Chmod(dir.path, mode), "failed to chmod %s", dir.path)
	}
	keepMode := !verify && dir.wasCreated && dir.created == dir.stat.mode
	return e.setPathStat(dir.path, dir.stat, keepMode)
}

// applyDelayedLinks replaces each placeholder still in place with its link, in archive order.
func (e *tarExtractor) applyDelayedLinks() error {
	for _, link := range e.delayed {
		if link.replaced {
			continue
		}
		info, err := os.Lstat(link.path)
		if err != nil || idOf(info) != link.id {
			// An entry reached the placeholder through another path and replaced it.
			continue
		}
		if err := removeEntry(link.path); err != nil {
			return err
		}
		if !link.symlink {
			if err := os.Link(link.target, link.path); err != nil {
				return errors.Wrapf(err, "failed to link %s to %s", link.path, link.target)
			}
			continue
		}
		if err := os.Symlink(link.target, link.path); err != nil {
			return errors.Wrapf(err, "failed to create symlink %s", link.path)
		}
		if err := e.setPathStat(link.path, link.stat, true); err != nil {
			return err
		}
	}
	return nil
}
