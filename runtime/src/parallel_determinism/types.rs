type ResourceId = String;
pub type TaskId = usize;
#[derive(Clone)]
pub struct Task {
    pub id: TaskId,
    pub name: String,
    pub reads: Vec<ResourceId>,
    pub writes: Vec<ResourceId>,
    pub work: &'static dyn Fn() -> Result<String, String>,
}

impl Task {
    pub fn conflicts_with(&self, other: &Task) -> bool {
        for read in &self.reads {
            if other.writes.contains(read) {
                return true;
            }
        }
        for write in &self.writes {
            if other.reads.contains(write) || other.writes.contains(write) {
                return true;
            }
        }
        false
    }
}
