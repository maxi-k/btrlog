use crate::trace::TaskPhaseTracker;
use std::mem::MaybeUninit;

#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub enum TaskState {
    Active,
    Waiting,
    #[default]
    None,
}

pub(crate) struct Task<T> {
    future: MaybeUninit<T>,
    state: TaskState,
    trace: TaskPhaseTracker,
}

impl<T> Task<T> {
    unsafe fn unwrap_unchecked(&mut self) -> &mut T {
        match self.state {
            TaskState::None => unsafe { std::hint::unreachable_unchecked() },
            _ => unsafe { self.future.assume_init_mut() },
        }
    }

    #[inline]
    fn new_active(future: T, name: String) -> Self {
        Self {
            future: MaybeUninit::new(future),
            state: TaskState::Active,
            trace: TaskPhaseTracker::new_active(name),
        }
    }

    fn activate(&mut self) {
        self.state = TaskState::Active;
        self.trace.on_wait_done();
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(self.state, TaskState::Active)
    }

    const fn none() -> Self {
        Self {
            future: MaybeUninit::uninit(),
            state: TaskState::None,
            trace: TaskPhaseTracker::zero(),
        }
    }
}

impl<T> Default for Task<T> {
    fn default() -> Self {
        Self {
            future: MaybeUninit::uninit(),
            state: Default::default(),
            trace: TaskPhaseTracker::zero(),
        }
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        match self.state {
            TaskState::None => {}
            _ => {
                self.state = TaskState::None;
                unsafe { self.future.assume_init_drop() };
            }
        }
    }
}

impl<T: std::ops::Deref<Target: Future> + std::ops::DerefMut> Task<std::pin::Pin<T>> {
    pub(crate) fn tracing_poll(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<<T::Target as Future>::Output> {
        debug_assert_eq!(self.state, TaskState::Active);
        let fut = unsafe { self.future.assume_init_mut() };
        self.trace.on_deactivate();
        let res = fut.as_mut().poll(cx);
        self.trace.on_poll_done();
        res
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct BoundedTaskList<T> {
    tasks: Vec<Task<T>>,
    active: Vec<u16>,
    free: Vec<u16>,
}

pub enum TaskAction {
    Dropped,
    Active,
    Waiting,
}

impl<T> BoundedTaskList<T> {
    pub fn new(iodepth: usize) -> Self {
        let mut free = vec![0u16; iodepth];
        let mut tasks = Vec::with_capacity(iodepth);
        for i in 0..iodepth {
            free[iodepth - i - 1] = i as u16;
            tasks.push(Task::none());
        }
        Self {
            tasks,
            active: Vec::with_capacity(iodepth),
            free,
        }
    }

    pub fn capacity(&self) -> usize {
        self.tasks.capacity()
    }

    pub fn tasks_in_system(&self) -> usize {
        self.tasks.capacity() - self.free.len()
    }

    pub fn active_queue_len(&self) -> usize {
        self.active.len()
    }

    #[inline]
    pub(crate) fn consume_task_id_list<F: FnMut(u16, &mut T, TaskState) -> TaskAction>(
        &mut self,
        list: &mut Vec<u16>,
        mut f: F,
    ) -> usize {
        let len = list.len();
        for (len, idx) in list.iter().enumerate() {
            let idx = *idx;
            let task_ref = unsafe { self.tasks.get_unchecked_mut(idx as usize) };
            let tstate = task_ref.state;
            debug_assert_ne!(tstate, TaskState::None); // task id lists only contain active tasks
            let task = unsafe { task_ref.unwrap_unchecked() };
            match f(idx, task, tstate) {
                TaskAction::Dropped => {
                    self.free.push(idx);
                    Self::print_task_if_long(task_ref, len);
                    *task_ref = Task::none();
                }
                TaskAction::Active => {
                    task_ref.activate();
                    self.active.push(idx);
                }
                TaskAction::Waiting => {
                    task_ref.state = TaskState::Waiting;
                    // task is external now; we assume that a waker will
                    // reinsert it into the active list at some point. If the
                    // waker doesn't do this, the task is 'leaked'.
                }
            }
        }
        list.clear();
        len
    }

    #[inline]
    pub(crate) fn consume_task_id_list_direct<F: FnMut(u16, &mut Task<T>) -> TaskAction>(
        &mut self,
        list: &mut Vec<u16>,
        mut f: F,
    ) -> usize {
        let len = list.len();
        for (len, idx) in list.iter().enumerate() {
            let idx = *idx;
            let task_ref = unsafe { self.tasks.get_unchecked_mut(idx as usize) };
            debug_assert_ne!(task_ref.state, TaskState::None); // task id lists only contain active tasks
            match f(idx, task_ref) {
                TaskAction::Dropped => {
                    self.free.push(idx);
                    Self::print_task_if_long(&task_ref, len);
                    *task_ref = Task::none();
                }
                TaskAction::Active => {
                    task_ref.activate();
                    self.active.push(idx);
                }
                TaskAction::Waiting => {
                    task_ref.state = TaskState::Waiting;
                    // task is external now; we assume that a waker will
                    // reinsert it into the active list at some point. If the
                    // waker doesn't do this, the task is 'leaked'.
                }
            }
        }
        list.clear();
        len
    }

    #[inline]
    pub fn take_active(&mut self, buffer: &mut Vec<u16>) {
        debug_assert!(buffer.is_empty());
        debug_assert!(buffer.capacity() >= self.active.capacity());
        std::mem::swap(&mut self.active, buffer);
    }

    #[inline]
    pub fn activate_task(&mut self, idx: u16) {
        let t = unsafe { self.tasks.get_unchecked_mut(idx as usize) };
        match t.state {
            TaskState::Active => {
                log::trace!("task {} woken but already active, not activating again", idx)
            }
            TaskState::Waiting => {
                t.activate();
                self.active.push(idx);
                log::trace!(
                    "task {} woken, reinserting into active; {} active, {} free, {} waiting",
                    idx,
                    self.active.len(),
                    self.free.len(),
                    (self.tasks.len() - self.active.len() - self.free.len())
                );
            }
            TaskState::None => unsafe { std::hint::unreachable_unchecked() },
        }
    }

    #[inline]
    pub fn try_add_task(&mut self, name: String, fut: T) -> Result<u16, T> {
        let id = match self.free.pop() {
            Some(id) => id,
            None => return Err(fut),
        };
        let slot = unsafe { self.tasks.get_unchecked_mut(id as usize) };
        debug_assert!(matches!(slot.state, TaskState::None));
        *slot = Task::new_active(fut, name);
        self.active.push(id);
        Ok(id)
    }

    #[inline]
    pub fn add_task_or_panic(&mut self, name: String, fut: T) -> u16 {
        match self.try_add_task(name, fut) {
            Ok(id) => id,
            Err(_) => {
                panic!(
                    "No more free task slots! {} active, {} free, {} waiting",
                    self.active.len(),
                    self.free.len(),
                    (self.tasks.len() - self.active.len() - self.free.len())
                );
            }
        }
    }

    pub fn task_stats_str(&self) -> String {
        format!(
            "(TaskList :active {} :free {} :waiting {})",
            self.active.len(),
            self.free.len(),
            (self.tasks.len() - self.active.len() - self.free.len())
        )
    }

    pub(crate) fn print_task_if_long(task: &Task<T>, context: usize) {
        let (poll, active, wait, polls) = task.trace.as_micros();
        let sum = poll + active + wait;
        if sum >= 1000.0 {
            log::warn!(
                "(LongTask :name {} :t {} :poll {} :active {} :wait {} :polls {} :context {})",
                task.trace.name(),
                sum,
                poll,
                active,
                wait,
                polls,
                context
            );
        } else if sum >= 100.0 {
            log::trace!(
                "(LongTask :name {} :t {} :poll {} :active {} :wait {} :polls {} :context {})",
                task.trace.name(),
                sum,
                poll,
                active,
                wait,
                polls,
                context
            );
        }
    }
}
