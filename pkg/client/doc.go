// Package client is our own smart cluster client.
//
// Iteration 11 (this version) covers what live resharding needs: a cached
// slot map, MOVED (update the map, refresh it in the background), ASK
// (ASKING + retry once on the other node, map unchanged), and short retries
// for TRYAGAIN / CLUSTERDOWN. Iteration 12 adds retries with backoff for
// network errors, read-from-replica and better pooling.
package client
