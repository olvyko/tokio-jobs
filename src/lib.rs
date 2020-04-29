use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

enum JobType {
    Once {
        deadline: DateTime<Utc>,
        done: bool,
    },
    Periodically {
        interval: ChronoDuration,
        last_run: Option<DateTime<Utc>>,
    },
}

pub struct Job {
    id: Uuid,
    job_type: JobType,
    func: Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync>,
}

pub async fn test() {}

impl Job {
    pub fn once(
        deadline: DateTime<Utc>,
        func: Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync>,
    ) -> Job {
        Job {
            id: Uuid::new_v4(),
            job_type: JobType::Once {
                deadline,
                done: false,
            },
            func,
        }
    }

    pub fn periodically(
        interval: Duration,
        func: Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync>,
    ) -> Job {
        Job {
            id: Uuid::new_v4(),
            job_type: JobType::Periodically {
                interval: ChronoDuration::from_std(interval).unwrap(),
                last_run: None,
            },
            func,
        }
    }

    pub fn is_done(&self) -> bool {
        match self.job_type {
            JobType::Once { deadline: _, done } => done,
            _ => false,
        }
    }

    pub async fn tick(&mut self) {
        match self.job_type {
            JobType::Once {
                deadline,
                ref mut done,
            } => {
                if *done {
                    return;
                };

                if deadline <= Utc::now() {
                    (self.func)().await;
                    *done = true;
                };
            }
            JobType::Periodically {
                interval,
                ref mut last_run,
            } => {
                if last_run.is_none()
                    || last_run.unwrap().signed_duration_since(Utc::now()) <= interval
                {
                    (self.func)().await;
                    *last_run = Some(Utc::now());
                };
            }
        }
    }
}

pub struct JobExecutor {
    jobs: Arc<RwLock<Vec<Job>>>,
}

impl JobExecutor {
    pub fn new() -> JobExecutor {
        JobExecutor {
            jobs: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn add(&self, job: Job) -> Uuid {
        let job_id = job.id;
        self.jobs.write().await.push(job);
        job_id
    }

    pub async fn jobs_loop(&self, interval: Duration) {
        loop {
            {
                let mut jobs = self.jobs.write().await;
                let mut done_jobs = Vec::new();
                for (i, job) in jobs.iter_mut().enumerate() {
                    job.tick().await;
                    if job.is_done() {
                        done_jobs.push(i);
                    };
                }
                for i in done_jobs {
                    jobs.remove(i);
                }
            }
            tokio::time::delay_for(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::time::Duration;

    fn hello_job() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        println!("Hello job PERIODICALLY");
        Box::pin(async {})
    }

    fn once_hello_job() -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        println!("Hello job ONCE");
        Box::pin(async {})
    }

    #[tokio::test]
    async fn test_job_executor() {
        let job_executor = Arc::new(JobExecutor::new());
        let job_executor_clone = job_executor.clone();
        let handle = tokio::task::spawn(async move {
            job_executor_clone.jobs_loop(Duration::from_secs(1)).await;
        });
        job_executor
            .add(Job::once(
                Utc::now() + ChronoDuration::seconds(5),
                Box::new(once_hello_job),
            ))
            .await;
        job_executor
            .add(Job::periodically(
                Duration::from_secs(2),
                Box::new(hello_job),
            ))
            .await;
        handle.await.unwrap();
    }
}
