package orker

import "os"

// ownerOf renders a file's owner for a message. Windows reports no numeric
// uid and gid, so it renders nothing.
func ownerOf(os.FileInfo) string { return "" }

// ownerUID reports no owner on Windows, for the same reason.
func ownerUID(os.FileInfo) (uint32, bool) { return 0, false }
