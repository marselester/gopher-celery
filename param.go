package celery

import "reflect"

// NewTaskParam returns a task param which facilitates access to args and kwargs.
func NewTaskParam(args []interface{}, kwargs map[string]interface{}) *TaskParam {
	return &TaskParam{
		args:   args,
		kwargs: kwargs,
	}
}

// TaskParam provides access to task's positional and keyword arguments.
// A task function might not know upfront how parameters will be supplied from the caller.
// They could be passed as positional arguments f(2, 3),
// keyword arguments f(a=2, b=3) or a mix of both f(2, b=3).
// In this case the arguments should be named and accessed by name,
// see NameArgs and Get methods.
//
// Methods prefixed with Must panic if they can't find an argument name
// or can't cast it to the corresponding type.
// The panic is logged by a worker and it doesn't affect other tasks.
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
	p.argNames = make(map[string]int, len(p.args))

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
	if !ok || pos >= len(p.args) {
		return nil, false
	}

	return p.args[pos], true
}

// MustString looks up a parameter by name and casts it to string.
// It panics if a parameter is missing or of a wrong type.
func (p *TaskParam) MustString(name string) string {
	v, ok := p.Get(name)
	if !ok {
		panic("param not found")
	}
	return v.(string)
}

// MustInt looks up a parameter by name and casts it to integer.
// It panics if a parameter is missing or of a wrong type.
func (p *TaskParam) MustInt(name string) int {
	v, ok := p.Get(name)
	if !ok {
		panic("param not found")
	}

	switch reflect.TypeOf(v).Kind() {
	case reflect.Float64:
		return int(v.(float64))
	default:
		return v.(int)
	}
}

// MustFloat looks up a parameter by name and casts it to float.
// It panics if a parameter is missing or of a wrong type.
func (p *TaskParam) MustFloat(name string) float64 {
	v, ok := p.Get(name)
	if !ok {
		panic("param not found")
	}
	return v.(float64)
}

// MustBool looks up a parameter by name and casts it to boolean.
// It panics if a parameter is missing or of a wrong type.
func (p *TaskParam) MustBool(name string) bool {
	v, ok := p.Get(name)
	if !ok {
		panic("param not found")
	}
	return v.(bool)
}
