// Package migrations embeds the SQL schema files so the server can apply them on
// startup without shipping loose files alongside the binary.
package migrations

import "embed"

// FS holds the numbered *.sql migrations, applied in lexical order.
//
//go:embed *.sql
var FS embed.FS
