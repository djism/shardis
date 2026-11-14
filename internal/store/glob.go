package store

// Match reports whether s matches a Redis-style glob pattern (used by KEYS
// and SCAN MATCH). Pattern syntax:
//
//	h*llo     '*' matches any run of characters, including none
//	h?llo     '?' matches exactly one character
//	h[ae]llo  one of a or e;  h[^e]llo: anything but e;  h[a-z]llo: a range
//	h\*llo    backslash makes the next character literal
//
// It's written from the documented behavior, not Redis's code. The
// algorithm remembers only the most recent '*' and retries from there, so it
// runs in O(len(pattern) * len(s)) even for nasty patterns like
// "*a*a*a*a*b". A naive recursive matcher goes exponential on those, which
// is a real denial-of-service risk for a server that takes patterns from clients.
func Match(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, 0 // position of the last '*' and where it started matching
	for i < len(s) {
		if p < len(pattern) {
			switch c := pattern[p]; c {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if ok, n := matchClass(pattern[p:], s[i]); n > 0 {
					if ok {
						p += n
						i++
						continue
					}
				} else if s[i] == '[' { // unterminated class: treat '[' literally
					p++
					i++
					continue
				}
			case '\\':
				if p+1 < len(pattern) {
					if pattern[p+1] == s[i] {
						p += 2
						i++
						continue
					}
				} else if s[i] == '\\' {
					p++
					i++
					continue
				}
			default:
				if c == s[i] {
					p++
					i++
					continue
				}
			}
		}
		// Mismatch: let the last '*' swallow one more character and retry.
		if starP >= 0 {
			starI++
			i = starI
			p = starP + 1
			continue
		}
		return false
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass matches c against a class starting at pat[0] == '['. It returns
// whether c matched and how many pattern bytes the class used (0 if there's
// no closing ']').
func matchClass(pat string, c byte) (bool, int) {
	i := 1
	negate := i < len(pat) && pat[i] == '^'
	if negate {
		i++
	}
	matched := false
	for i < len(pat) && pat[i] != ']' {
		switch {
		case pat[i] == '\\' && i+1 < len(pat):
			matched = matched || pat[i+1] == c
			i += 2
		case i+2 < len(pat) && pat[i+1] == '-' && pat[i+2] != ']':
			lo, hi := pat[i], pat[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			matched = matched || (c >= lo && c <= hi)
			i += 3
		default:
			matched = matched || pat[i] == c
			i++
		}
	}
	if i >= len(pat) {
		return false, 0
	}
	return matched != negate, i + 1
}
