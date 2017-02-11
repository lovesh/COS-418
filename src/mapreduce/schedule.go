package mapreduce

import (
	"github.com/willf/bitset"
	"sync"
	"math/rand"
)


/*
The approach of scheduling: No 2 phases run in parallel. During each phase a bitmap of workers is maintained in which
a set bit indicates that the worker is doing a task and a clear bit indicates worker is idle. Each phase has a
`started` bitset to track which tasks have been given to worker, if a bit is unset it either was not given or the
worker to which it was given failed so it must be re-assigned to other worker.
 */


// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	done := make(chan bool)
	workers := new(Workers)
	workers.names = mr.workers
	workers.busyness = bitset.New(uint(len(workers.names)))
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
		//go startMap(mr, workers, ntasks, nios, done)
		go startPhase(phase, mr, workers, ntasks, nios, done)
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		//go startReduce(mr, workers, ntasks, nios, done)
		go startPhase(phase, mr, workers, ntasks, nios, done)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	<-done
	debug("Schedule: %v phase done\n", phase)
}


type TaskResult struct {
	status   int	// -ve if task failed, +ve if succeeded, absolute value indicated the task number
	workerName string
}

type Workers struct {
	sync.Mutex
	names    []string
	busyness *bitset.BitSet
}

func (w *Workers) add(worker string) {
	w.Lock()
	defer w.Unlock()
	w.names = append(w.names, worker)
	newBsy := bitset.New(uint(len(w.names)))
	w.busyness.Copy(newBsy)
	w.busyness = newBsy
	//w.busyness.extendSetMaybe(uint(len(w.names)))
}

func (w *Workers) free(worker string) {
	w.setBusyness(worker, false)
}

func (w *Workers) busy(worker string) {
	w.setBusyness(worker, true)
}


func (w *Workers) setBusyness(worker string, b bool) {
	w.Lock()
	defer w.Unlock()
	for i, nm := range w.names {
		if nm == worker	{
			if b {
				w.busyness.Set(uint(i))
			} else {
				w.busyness.Clear(uint(i))
			}
			break
		}
	}
}

func (w *Workers) nextIdle() (string, bool) {
	w.Lock()
	defer w.Unlock()
	idleCount := uint(len(w.names)) - w.busyness.Count()
	if idleCount == 0 {
		debug("No idle worker\n")
		debug("Set bits %d \n", w.busyness.Count())
		return "", false
	} else {
		// Choosing a random worker
		var idleIndexes []uint
		var i uint = 0
		for {
			idx, ok := w.busyness.NextClear(i)
			if !ok {
				break
			} else {
				idleIndexes = append(idleIndexes, idx)
				if idleCount == uint(len(idleIndexes)) {
					break
				}
			}
			i = idx + 1
		}
		idx := idleIndexes[rand.Intn(len(idleIndexes))]
		return w.names[idx], true
	}
}

func assignToWorker(mr *Master, workers *Workers, started *bitset.BitSet, mutex *sync.Mutex, phase jobPhase,
	ntasks int, nios int, statusChan chan TaskResult) {
	/*
	 Checks if there is any task to be assigned to assigned to a worker and if any worker is idle, assign the
	 task to that worker
	 */

	// Find next clear bit from position 1, the bit at position 0 is unused, since for every task number a +ve or
	// -ve value is needed depending on success or failure of task and -0 does not make sense
	n, ok := started.NextClear(1)

	if !ok || n > uint(ntasks) {
		debug("All tasks in progress or done\n")
		return
	} else {
		w, ok := workers.nextIdle()
		if !ok {
			debug("All workers busy, no task assignment done\n")
			return
		}
		mutex.Lock()
		started.Set(uint(n))
		workers.busy(w)
		mutex.Unlock()
		go func(w string, statusChan chan TaskResult, taskNumber uint) {
			args := new(DoTaskArgs)
			args.JobName = mr.jobName
			if phase == mapPhase {
				args.File = mr.files[taskNumber-1]
			}
			args.TaskNumber = int(taskNumber-1)
			args.Phase = phase
			args.NumOtherPhase = nios
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			if ok {
				statusChan <- TaskResult{int(taskNumber), w}
			} else {
				statusChan <- TaskResult{int(-taskNumber), w}
			}
		}(w, statusChan, n)
	}
}


func startPhase(phase jobPhase, mr *Master, workers *Workers, ntasks int, nouts int, done chan bool) {
	debug("Starting %s: %d tasks, %d outs\n", phase, ntasks, nouts)
	started := bitset.New(uint(ntasks+1))
	finished := 0
	statusChan := make(chan TaskResult)
	var mutex = &sync.Mutex{}
	defer close(statusChan)

	assignToWorker(mr, workers, started, mutex, phase, ntasks, nouts, statusChan)

Loop:
	for {
		select {
		case w := <- mr.registerChannel:
			workers.add(w)
			assignToWorker(mr, workers, started, mutex, phase, ntasks, nouts, statusChan)

		case tr := <- statusChan:
			if tr.status > 0 {
				debug("%s Task %d done by worker %s\n", phase, tr.status, tr.workerName)
				mutex.Lock()
				finished++
				mutex.Unlock()
			} else {
				debug("%s Task %d not done by worker %s\n", phase, -tr.status, tr.workerName)
				mutex.Lock()
				started.Clear(uint(-tr.status))
				mutex.Unlock()
			}
			// The worker is being freed, but in the current setting, the workers dont restart after they
			// crash so they dont need to be marked free but if workers were restarting after crash,
			// freeing after failure should definitely happen
			debug("Freeing %s worker %s\n", phase, tr.workerName)
			workers.free(tr.workerName)
			if finished == ntasks {
				debug("All %s tasks done\n", phase)
				break Loop
			} else {
				assignToWorker(mr, workers, started, mutex, phase, ntasks, nouts, statusChan)
			}
		}
	}
	done <- true
}
