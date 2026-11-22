package client_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

type tarEntry struct {
	hdr  tar.Header
	body string
	// rawName replaces the encoded name, for names archive/tar refuses to write.
	rawName string
}

func regular(name string, mode int64, body string) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: mode}, body: body}
}

func directory(name string, mode int64) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeDir, Name: name, Mode: mode}}
}

func symlink(name, target string) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeSymlink, Name: name, Linkname: target, Mode: 0o777}}
}

func hardlink(name, target string) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeLink, Name: name, Linkname: target}}
}

func regularWithSlash(name string, mode int64) tarEntry {
	placeholder := strings.TrimSuffix(name, "/") + "_"
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeReg, Name: placeholder, Mode: mode}, rawName: name}
}

func globalHeader(records map[string]string) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: tar.TypeXGlobalHeader, Name: "global", PAXRecords: records}}
}

func wholeSecond(entry tarEntry) tarEntry {
	// Whole seconds fit the USTAR field, so the member has no mtime record of its own.
	entry.hdr.ModTime = time.Date(2019, 1, 2, 3, 4, 5, 0, time.UTC)
	return entry
}

func typed(flag byte, name string, mode int64, body string) tarEntry {
	return tarEntry{hdr: tar.Header{Typeflag: flag, Name: name, Mode: mode}, body: body}
}

// extractCase is an archive plus the tree it is extracted over.
type extractCase struct {
	name    string
	setup   func(t *testing.T, dir string)
	entries []tarEntry
}

func TestExtractMatchesGNUTar(t *testing.T) {
	if out, err := exec.Command("tar", "--version").Output(); err != nil || !bytes.Contains(out, []byte("GNU tar")) {
		t.Skip("GNU tar is not installed")
	}
	if os.Geteuid() == 0 {
		t.Skip("ownership differs as root; cases assume an unprivileged user")
	}
	// Large enough to be written in several chunks.
	large := strings.Repeat("0123456789abcdef", 9<<16)
	cases := []extractCase{
		{name: "Tree", entries: []tarEntry{
			directory("./", 0o750),
			directory("a/", 0o755),
			regular("a/file", 0o640, "hello"),
			regular("a/exec", 0o755, "#!/bin/sh\n"),
			regular("a/empty", 0o600, ""),
			regular("a/large", 0o644, large),
			regular("a/setuid", 0o4755, "x"),
			directory("a/setgid/", 0o2775),
			directory("a/setgid/child/", 0o700),
			regular("a/setgid/implicit/file", 0o644, "y"),
			directory("a/sticky/", 0o1777),
			directory("readonly/", 0o555),
			regular("readonly/file", 0o444, "ro"),
			regular("missing/parents/file", 0o644, "z"),
			regular("./dot//slashes/./file", 0o644, "dots"),
		}},
		{name: "Links", entries: []tarEntry{
			directory("dir/", 0o755),
			regular("dir/small", 0o644, "small"),
			regular("dir/large", 0o644, large),
			symlink("relative", "dir/small"),
			symlink("dangling", "nowhere"),
			symlink("absolute", "/etc"),
			symlink("dotdot", "../outside"),
			symlink("dir-link", "dir"),
			regular("dir-link/through", 0o644, "via symlink"),
			hardlink("hard-small", "dir/small"),
			hardlink("hard-large", "dir/large"),
			hardlink("hard-absolute", "absolute"),
			regular("dir/small", 0o600, "replaced"),
			hardlink("hard-rooted", "/dir/small"),
			hardlink("hard-escaping", "../../dir/small"),
			typed(tar.TypeFifo, "fifo", 0o640, ""),
		}},
		{name: "Duplicates", entries: []tarEntry{
			regular("file", 0o644, "first"),
			regular("file", 0o600, "second"),
			regular("large-then-small", 0o644, large),
			hardlink("link-to-large", "large-then-small"),
			regular("large-then-small", 0o600, "small"),
			regular("small-then-large", 0o644, "small"),
			regular("small-then-large", 0o600, large),
			regular("file-then-link", 0o644, "x"),
			symlink("file-then-link", "file"),
			symlink("link-then-file", "file"),
			regular("link-then-file", 0o644, "replaced"),
			regular("file-then-dir", 0o644, "x"),
			directory("file-then-dir/", 0o755),
			directory("dir-then-file/", 0o755),
			regular("dir-then-file", 0o644, "x"),
			symlink("unsafe-twice", "/first"),
			symlink("unsafe-twice", "/second"),
			directory("real/", 0o755),
			symlink("link-then-dir", "real"),
			directory("link-then-dir/", 0o700),
			directory("twice/", 0o755),
			directory("twice/", 0o700),
		}},
		{name: "UnusualHeaders", entries: []tarEntry{
			typed(tar.TypeXGlobalHeader, "global", 0, ""),
			typed('V', "volume", 0, ""),
			regularWithSlash("trailing-slash/", 0o755),
			typed('Z', "unknown-type", 0o644, "data"),
			regular("/absolute/file", 0o644, "abs"),
		}},
		{name: "GlobalHeaders", entries: []tarEntry{
			globalHeader(map[string]string{"mtime": "1500000000.5", "linkpath": "global-target"}),
			wholeSecond(regular("global-mtime", 0o644, "x")),
			regular("own-mtime-record", 0o644, "x"),
			wholeSecond(symlink("global-linkpath", "own-target")),
			globalHeader(map[string]string{"comment": "replaces the first global header"}),
			wholeSecond(regular("after-replacing-global", 0o644, "x")),
			globalHeader(map[string]string{"mtime": "1400000000"}),
			wholeSecond(regular("global-mtime-again", 0o644, "x")),
			globalHeader(map[string]string{}),
			wholeSecond(regular("after-empty-global", 0o644, "x")),
		}},
		{name: "ExistingTree", setup: existingTree, entries: []tarEntry{
			directory("symlink-to-dir/", 0o755),
			regular("symlink-to-dir/file", 0o644, "x"),
			regular("symlinked-parent/file", 0o644, "into target"),
			regular("dangling", 0o644, "x"),
			regular("readonly", 0o644, "rewritten"),
			symlink("unsafe", "/first"),
			symlink("stale", "/second"),
		}},
		{name: "OutOfOrder", entries: []tarEntry{
			directory("revisited/", 0o755),
			regular("revisited/first", 0o644, "x"),
			directory("other/", 0o755),
			regular("revisited/later", 0o644, "x"),
			directory("./mixed/", 0o755),
			regular("mixed/file", 0o644, "x"),
			directory("link-parent/", 0o755),
			symlink("link-parent/unsafe", "/absolute"),
			regular("after-link", 0o644, "x"),
		}},
		{name: "AliasedDirectory", entries: []tarEntry{
			directory("real/", 0o755),
			symlink("alias", "real"),
			directory("alias/replaced/", 0o755),
			regular("real/replaced", 0o644, "x"),
			directory("alias/written/", 0o700),
			regular("real/written/file", 0o644, "x"),
		}},
		{name: "TrailingDot", entries: []tarEntry{
			directory("created/./", 0o555),
			regular("created/file", 0o644, "x"),
			directory("nested/created/././", 0o555),
			regular("nested/created/file", 0o644, "x"),
			regular("existing/before", 0o644, "x"),
			directory("existing/./", 0o700),
			regular("existing/after", 0o644, "x"),
			regular("aliased/before", 0o644, "x"),
			directory("./aliased/", 0o700),
			regular("aliased/after", 0o644, "x"),
		}},
		{name: "TrailingDotOverDirectory", entries: []tarEntry{
			directory("d/", 0o755),
			directory("d/./", 0o555),
			regular("d/file", 0o644, "x"),
		}},
		{name: "RevisitReadOnly", entries: []tarEntry{
			directory("readonly/", 0o555),
			regular("elsewhere", 0o644, "x"),
			regular("readonly/late", 0o644, "x"),
		}},
		{name: "Rewrites", entries: rewrites()},
		{name: "DotDotName", entries: []tarEntry{regular("a/../escape", 0o644, "x")}},
		{name: "ThroughUnsafeLink", entries: []tarEntry{symlink("u", "/tmp"), regular("u/file", 0o644, "x")}},
		{name: "ThroughLinkToUnsafeLink", entries: []tarEntry{
			symlink("u", "/tmp"),
			symlink("s", "u"),
			regular("s/file", 0o644, "x"),
		}},
		{name: "UnderExistingFile", setup: existingTree, entries: []tarEntry{
			regular("file-over-dir-parent/file", 0o644, "x"),
		}},
		{name: "FileOverNonEmptyDir", entries: []tarEntry{regular("d/f", 0o644, "x"), regular("d", 0o644, "x")}},
		{name: "FileOverTargetDir", entries: []tarEntry{regular(".", 0o644, "x")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archive := buildTar(t, tc.entries)
			gnuDir, goDir := t.TempDir(), t.TempDir()
			t.Cleanup(func() { makeWritable(t, gnuDir); makeWritable(t, goDir) })
			if tc.setup != nil {
				tc.setup(t, gnuDir)
				tc.setup(t, goDir)
			}

			gnuTar := exec.Command("tar", "-xpf", "-", "-C", gnuDir)
			gnuTar.Stdin = bytes.NewReader(archive)
			gnuOut, gnuErr := gnuTar.CombinedOutput()
			goErr := client.Extract(t.Context(), bytes.NewReader(zstdCompress(t, archive)), goDir, 0)

			assert.Equal(t, gnuErr == nil, goErr == nil, "GNU tar: %v %s\nExtract: %v", gnuErr, gnuOut, goErr)
			if gnuErr == nil {
				assert.Equal(t, treeState(t, gnuDir), treeState(t, goDir))
			}
		})
	}
}

func TestExtractLongMemberNames(t *testing.T) {
	if out, err := exec.Command("tar", "--version").Output(); err != nil || !bytes.Contains(out, []byte("GNU tar")) {
		t.Skip("GNU tar is not installed")
	}
	// Each name fits in PATH_MAX but not once joined to the destination, which GNU tar resolves names against.
	deep := strings.TrimSuffix(strings.Repeat(strings.Repeat("d", 200)+"/", 20), "/")
	archive := buildTar(t, []tarEntry{
		directory(deep+"/", 0o750),
		regular(deep+"/file", 0o640, "x"),
		regular(deep+"/replaced", 0o644, "x"),
		regular(deep+"/replaced", 0o600, "y"),
		symlink(deep+"/link", "file"),
		hardlink(deep+"/hard", deep+"/file"),
		symlink(deep+"/absolute", "/elsewhere"),
	})
	prefix := strings.Repeat("p", 200)
	gnuDir, goDir := filepath.Join(t.TempDir(), prefix), filepath.Join(t.TempDir(), prefix)
	assert.NoError(t, os.Mkdir(gnuDir, 0o755))
	assert.NoError(t, os.Mkdir(goDir, 0o755))

	gnuTar := exec.Command("tar", "-xpf", "-", "-C", gnuDir)
	gnuTar.Stdin = bytes.NewReader(archive)
	out, err := gnuTar.CombinedOutput()
	assert.NoError(t, err, "%s", out)
	assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(zstdCompress(t, archive)), goDir, 0))
	assert.Equal(t, treeState(t, gnuDir), treeState(t, goDir))
}

func TestExtractRejectsGlobalDataLayoutRecords(t *testing.T) {
	// GNU tar reads member data using these, but archive/tar has already sized the member from its own header.
	for _, key := range []string{"size", "GNU.sparse.realsize"} {
		t.Run(key, func(t *testing.T) {
			entries := []tarEntry{globalHeader(map[string]string{key: "1"}), regular("file", 0o644, "data")}
			archive := zstdCompress(t, buildTar(t, entries))
			err := client.Extract(t.Context(), bytes.NewReader(archive), t.TempDir(), 0)
			assert.Error(t, err)
		})
	}
}

func TestExtractRejectsIncompleteArchive(t *testing.T) {
	archive := buildTar(t, []tarEntry{wholeSecond(regular("file", 0o644, "data"))})
	for name, data := range map[string][]byte{
		"Empty":          nil,
		"MissingPadding": archive[:512+len("data")],
	} {
		t.Run(name, func(t *testing.T) {
			err := client.Extract(t.Context(), bytes.NewReader(zstdCompress(t, data)), t.TempDir(), 0)
			assert.Error(t, err)
		})
	}
}

func TestExtractLaterEntryReplacesUnsafeSymlink(t *testing.T) {
	// GNU tar finds placeholders by inode number, so a file reusing a freed placeholder's inode confuses it.
	cases := []struct {
		name    string
		entries []tarEntry
		want    string
	}{
		{"File", []tarEntry{symlink("entry", "/first"), regular("entry", 0o644, "file")}, "file"},
		{"LinkAfterFile", []tarEntry{
			symlink("entry", "/first"), regular("entry", 0o644, "file"), symlink("entry", "/second"),
		}, "-> /second"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			archive := zstdCompress(t, buildTar(t, tc.entries))
			assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(archive), dir, 0))
			got, err := os.Readlink(filepath.Join(dir, "entry"))
			if err == nil {
				got = "-> " + got
			} else {
				data, readErr := os.ReadFile(filepath.Join(dir, "entry"))
				assert.NoError(t, readErr)
				got = string(data)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// rewrites writes many small files twice, so queued writes must land in archive order.
func rewrites() []tarEntry { //nolint:privatedoc // explains why
	var entries []tarEntry
	for round := range 2 {
		for i := range 500 {
			entries = append(entries, regular(fmt.Sprintf("many/%03d", i), 0o644, fmt.Sprintf("round %d file %d", round, i)))
			if round == 0 && i%50 == 0 {
				entries = append(entries, hardlink(fmt.Sprintf("many/link-%03d", i), fmt.Sprintf("many/%03d", i)))
			}
		}
	}
	return entries
}

func existingTree(t *testing.T, dir string) {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "file-over-dir-parent"), []byte("x"), 0o644))
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "real"), 0o755))
	assert.NoError(t, os.Symlink("real", filepath.Join(dir, "symlink-to-dir")))
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "target"), 0o755))
	assert.NoError(t, os.Symlink("target", filepath.Join(dir, "symlinked-parent")))
	assert.NoError(t, os.Symlink("nowhere", filepath.Join(dir, "dangling")))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "readonly"), []byte("old"), 0o444))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "stale"), []byte("old"), 0o644))
}

func archiveTime() time.Time {
	return time.Date(2020, 1, 2, 3, 4, 5, 123456789, time.UTC)
}

func buildTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, entry := range entries {
		hdr := entry.hdr
		if hdr.ModTime.IsZero() {
			hdr.ModTime = archiveTime()
		}
		hdr.Format = tar.FormatPAX
		hdr.Size = int64(len(entry.body))
		if hdr.Typeflag == tar.TypeXGlobalHeader {
			records := hdr.PAXRecords
			if records == nil {
				records = map[string]string{"comment": "x"}
			}
			hdr = tar.Header{Typeflag: tar.TypeXGlobalHeader, Name: hdr.Name, PAXRecords: records}
		}
		if entry.rawName != "" {
			// A single USTAR block, so the name can be patched in place.
			hdr.Format = tar.FormatUSTAR
			hdr.ModTime = hdr.ModTime.Truncate(time.Second)
		}
		assert.NoError(t, tw.Flush())
		offset := buf.Len()
		assert.NoError(t, tw.WriteHeader(&hdr))
		_, err := tw.Write([]byte(entry.body))
		assert.NoError(t, err)
		if entry.rawName != "" {
			assert.NoError(t, tw.Flush())
			patchName(buf.Bytes()[offset:offset+512], entry.rawName)
		}
	}
	assert.NoError(t, tw.Close())
	return buf.Bytes()
}

func patchName(block []byte, name string) {
	copy(block[:100], append([]byte(name), make([]byte, 100)...))
	copy(block[148:156], "        ")
	sum := 0
	for _, b := range block {
		sum += int(b)
	}
	copy(block[148:156], fmt.Sprintf("%06o\x00 ", sum))
}

func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	cmd := exec.Command("zstd", "-c")
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	assert.NoError(t, err)
	return out
}

// treeState describes every path under dir; only archived mtimes are comparable, the rest are extraction times.
//
// It resolves paths through an os.Root, so trees deeper than PATH_MAX can be compared.
func treeState(t *testing.T, dir string) map[string]string { //nolint:privatedoc // explains why
	t.Helper()
	root, err := os.OpenRoot(dir)
	assert.NoError(t, err)
	defer root.Close()
	state := map[string]string{}
	describeTree(t, root, ".", state)
	return state
}

func describeTree(t *testing.T, root *os.Root, name string, state map[string]string) {
	t.Helper()
	info, err := root.Lstat(name)
	assert.NoError(t, err)
	desc := fmt.Sprintf("%v nlink=%d", info.Mode(), info.Sys().(*syscall.Stat_t).Nlink) //nolint:forcetypeassert
	if !info.ModTime().After(archiveTime()) {
		desc += " mtime=" + info.ModTime().Format(time.RFC3339Nano)
	}
	switch {
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := root.Readlink(name)
		assert.NoError(t, err)
		desc += " -> " + target
	case info.Mode().IsRegular():
		data, err := root.ReadFile(name)
		assert.NoError(t, err)
		desc += fmt.Sprintf(" sha256=%x", sha256.Sum256(data))
	case info.IsDir():
		f, err := root.Open(name)
		assert.NoError(t, err)
		children, err := f.Readdirnames(-1)
		assert.NoError(t, errors.Join(err, f.Close()))
		for _, child := range children {
			describeTree(t, root, filepath.Join(name, child), state)
		}
	}
	state[name] = desc
}
