package orker

import "os"

// ownerOf renders a file's owner for a message. Windows reports no numeric
// uid and gid, so it renders nothing.
func ownerOf(os.FileInfo) string { return "" }
