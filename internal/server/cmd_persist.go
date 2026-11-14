package server

import (
	"errors"
	"strings"

	"github.com/djism/shardis/internal/persist"
)

// BGREWRITEAOF compacts the AOF in the background (a fuzzy snapshot, ADR 016).
func cmdBgRewriteAOF(cl *client, _ [][]byte) {
	aof := cl.srv.aof
	if aof == nil {
		cl.w.WriteError("ERR AOF is not enabled (start with --appendonly yes)")
		return
	}
	switch err := aof.Rewrite(cl.srv.engine); {
	case errors.Is(err, persist.ErrRewriteInProgress):
		cl.w.WriteError("ERR " + err.Error())
	case err != nil:
		cl.w.WriteError("ERR " + err.Error())
	default:
		cl.w.WriteSimple("Background append only file rewriting started")
	}
}

func infoPersistence(b *strings.Builder, s *Server) {
	field(b, "loading", 0)
	if s.aof == nil {
		field(b, "aof_enabled", 0)
		return
	}
	in := s.aof.Info()
	status := func(msg string) string {
		if msg == "" {
			return "ok"
		}
		return "err"
	}
	field(b, "aof_enabled", 1)
	field(b, "aof_fsync", in.Fsync)
	field(b, "aof_rewrite_in_progress", boolInt(in.RewriteRunning))
	field(b, "aof_rewrites", in.Rewrites)
	field(b, "aof_last_rewrite_time_sec", int64(in.LastRewrite.Seconds()))
	field(b, "aof_last_bgrewrite_status", status(in.LastRewriteError))
	field(b, "aof_last_write_status", status(in.WriteError))
	field(b, "aof_current_size", in.CurrentSize)
	field(b, "aof_base_size", in.BaseSize)
	field(b, "aof_pending_bytes", in.PendingBytes)
	field(b, "aof_fsyncs", in.Fsyncs)
	if in.LastFsyncAgo > 0 {
		field(b, "aof_last_fsync_ms_ago", in.LastFsyncAgo.Milliseconds())
	}
	field(b, "aof_loaded_records", in.Loaded.Records)
	field(b, "aof_load_ms", in.Loaded.Took.Milliseconds())
	field(b, "aof_load_truncated_bytes", in.Loaded.Truncated)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
