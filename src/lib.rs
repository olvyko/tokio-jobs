pub use chrono::{DateTime, Duration as ChronoDuration, NaiveTime, Utc};
use futures::future::{BoxFuture, Future};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

type BoxFn = Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>;

enum JobType {
    Once {
        deadline: DateTime<Utc>,
        done: bool,
    },
    Periodically {
        interval: ChronoDuration,
        last_run: Option<DateTime<Utc>>,
    },
    PeriodicallyAt {
        run_at: NaiveTime,
        was_run: bool,
    },
}

pub struct Job {
    id: Uuid,
    job_type: JobType,
    func: BoxFn,
}

impl Job {
    pub fn once<F, Fut>(deadline: DateTime<Utc>, f: F) -> Job
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Job {
            id: Uuid::new_v4(),
            job_type: JobType::Once {
                deadline,
                done: false,
            },
            func: Box::new(move || Box::pin(f())),
        }
    }

    pub fn periodically<F, Fut>(interval: Duration, f: F) -> Job
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Job {
            id: Uuid::new_v4(),
            job_type: JobType::Periodically {
                interval: ChronoDuration::from_std(interval).unwrap(),
                last_run: None,
            },
            func: Box::new(move || Box::pin(f())),
        }
    }

    pub fn periodically_at<F, Fut>(run_at: NaiveTime, f: F) -> Job
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Job {
            id: Uuid::new_v4(),
            job_type: JobType::PeriodicallyAt {
                run_at: run_at,
                was_run: false,
            },
            func: Box::new(move || Box::pin(f())),
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
            JobType::PeriodicallyAt {
                run_at,
                ref mut was_run,
            } => {
                let now = Utc::now().time();
                if !*was_run && now >= run_at {
                    (self.func)().await;
                    *was_run = true;
                };
                if *was_run && now < run_at {
                    *was_run = false;
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

    pub async fn event_loop(&self, interval: Duration) {
        loop {
            {
                let mut done_jobs = Vec::new();
                let mut jobs = self.jobs.write().await;
                for job in jobs.iter_mut() {
                    job.tick().await;
                    if job.is_done() {
                        done_jobs.push(job.id);
                    };
                }
                if !done_jobs.is_empty() {
                    for id in done_jobs {
                        jobs.retain(|job| job.id != id);
                    }
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

    struct Service;

    impl Service {
        pub fn new() -> Service {
            Service
        }

        pub async fn hello_job(&self) {
            println!("Hello job SERVICE SELF");
        }
    }

    async fn hello_job() {
        println!("Hello job PERIODICALLY");
    }

    async fn hello_job_at() {
        println!("Hello job PERIODICALLY AT {}", Utc::now().time());
    }

    async fn once_hello_job() {
        println!("Hello job ONCE");
    }

    #[tokio::test]
    async fn test_job_executor() {
        let job_executor = Arc::new(JobExecutor::new());
        let job_executor_clone = job_executor.clone();
        let handle = tokio::task::spawn(async move {
            job_executor_clone.event_loop(Duration::from_secs(1)).await;
        });

        let service = Arc::new(Service::new());
        let serive_clone = service.clone();

        job_executor
            .add(Job::once(
                Utc::now() + ChronoDuration::seconds(5),
                once_hello_job,
            ))
            .await;
        job_executor
            .add(Job::once(
                Utc::now() + ChronoDuration::seconds(5),
                move || {
                    let serive_clone = serive_clone.clone();
                    async move { serive_clone.hello_job().await }
                },
            ))
            .await;
        job_executor
            .add(Job::periodically(Duration::from_secs(2), hello_job))
            .await;
        let now = Utc::now().time();
        println!("Time now {}", now);
        let now_plus = now + ChronoDuration::seconds(5);
        job_executor
            .add(Job::periodically_at(now_plus, hello_job_at))
            .await;
        handle.await.unwrap();
    }
}
