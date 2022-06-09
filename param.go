package celery

// TaskParam provides access to task's positional and keyword arguments.
type TaskParam struct {
	// argNames is map of argument names to the respective args indices.
	argNames map[string]int
	// args are arguments.
	args []interface{}
	// kwargs are keyword arguments.
	kwargs map[string]interface{}
}

// Args returns task's positional arguments.
func (p *TaskParam) Args() []interface{} {
	return p.args
}

// Kwargs returns task's keyword arguments.
func (p *TaskParam) Kwargs() map[string]interface{} {
	return p.kwargs
}

// NameArgs assigns names to the task arguments.
func (p *TaskParam) NameArgs(name ...string) {
	for i := 0; i < len(name); i++ {
		p.argNames[name[i]] = i
	}
}

// Get returns a parameter by name.
// Firstly it tries to look it up in Kwargs,
// and then in Args if their names were provided by the client.
func (p *TaskParam) Get(name string) (v interface{}, ok bool) {
	if v, ok = p.kwargs[name]; ok {
		return v, true
	}

	var pos int
	pos, ok = p.argNames[name]
	if !ok || pos >= len(p.argNames) {
		return nil, false
	}

	return p.args[pos], true
}
