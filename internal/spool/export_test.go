package spool

const FileSuffix = fileSuffix

// Readers returns how many requests are serving key.
func (g *Group) Readers(key string) int {
	e := g.lookup(key)
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.readers
}

// Written returns how many body bytes the fill for key has produced.
func (g *Group) Written(key string) int64 {
	e := g.lookup(key)
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.written
}

// Len returns how many responses the group holds.
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.entries)
}

func (g *Group) lookup(key string) *entry {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.entries[key]
}
