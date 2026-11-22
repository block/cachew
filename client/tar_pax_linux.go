package client

import (
	"archive/tar"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/errors"
)

func checkGlobalPAX(records map[string]string) error {
	// These change where member data ends, which archive/tar has already decided from the member's own header.
	for key := range records {
		if key == "size" || strings.HasPrefix(key, "GNU.sparse.") {
			return errors.Errorf("unsupported %q record in a PAX global header", key)
		}
	}
	return nil
}

// applyGlobalPAX applies the records of the last PAX global header to hdr, as GNU tar does.
//
// archive/tar returns global headers without merging them into later members, and a member's own records win.
func applyGlobalPAX(hdr *tar.Header, records map[string]string) error {
	for key, value := range records {
		if _, ok := hdr.PAXRecords[key]; ok {
			continue
		}
		var err error
		switch key {
		case "path":
			// GNU tar ignores an empty path, keeping the name from the member's own header.
			if value != "" {
				hdr.Name = value
			}
		case "linkpath":
			hdr.Linkname = value
		case "uname":
			hdr.Uname = value
		case "gname":
			hdr.Gname = value
		case "uid":
			hdr.Uid, err = parsePAXID(value)
		case "gid":
			hdr.Gid, err = parsePAXID(value)
		case "mtime":
			hdr.ModTime, err = parsePAXTime(value)
		}
		if err != nil {
			return errors.Wrapf(err, "%s: invalid %q record in a PAX global header", hdr.Name, key)
		}
	}
	return nil
}

func parsePAXID(value string) (int, error) {
	// GNU tar accepts any uid_t, which is 32 bits on Linux; int is narrower on 32-bit platforms.
	id, err := strconv.ParseUint(value, 10, min(32, strconv.IntSize-1))
	return int(id), errors.WithStack(err)
}

func parsePAXTime(value string) (time.Time, error) {
	// Same format as archive/tar's unexported parser: decimal seconds with an optional fraction.
	secs, frac, _ := strings.Cut(value, ".")
	s, err := strconv.ParseInt(secs, 10, 64)
	if err != nil {
		return time.Time{}, errors.WithStack(err)
	}
	if frac == "" {
		return time.Unix(s, 0), nil
	}
	if strings.Trim(frac, "0123456789") != "" {
		return time.Time{}, errors.Errorf("invalid fraction %q", frac)
	}
	ns, _ := strconv.ParseInt((frac + "000000000")[:9], 10, 64) //nolint:errcheck // digits only
	if strings.HasPrefix(secs, "-") {
		ns = -ns
	}
	return time.Unix(s, ns), nil
}
