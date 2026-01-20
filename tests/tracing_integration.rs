#[cfg(feature = "tracing-integration")]
mod tests {
    use asupersync::runtime::RuntimeState;
    use asupersync::types::Budget;
    use std::sync::{Arc, Mutex};
    use tracing::Subscriber;
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::prelude::*;

    struct SpanRecorder {
        spans: Arc<Mutex<Vec<String>>>,
    }

    impl<S> Layer<S> for SpanRecorder
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &tracing::Id, _ctx: Context<'_, S>) {
            if attrs.metadata().name() == "region" {
                let mut spans = self.spans.lock().unwrap();
                spans.push(format!("region_new: {:?}", attrs));
            }
        }

        fn on_record(&self, _id: &tracing::Id, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
             let mut spans = self.spans.lock().unwrap();
             spans.push(format!("region_record: {:?}", values));
        }

        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut spans = self.spans.lock().unwrap();
            spans.push(format!("event: {:?}", event));
        }
    }

    #[test]
    fn verify_region_spans() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let recorder = SpanRecorder { spans: spans.clone() };
        
        let subscriber = tracing_subscriber::registry().with(recorder);
        
        tracing::subscriber::with_default(subscriber, || {
            let mut state = RuntimeState::new();
            let region = state.create_root_region(Budget::INFINITE);
            
            // Should have created a span
            {
                let spans = spans.lock().unwrap();
                assert!(!spans.is_empty(), "Should have recorded region span creation");
                let creation = spans.iter().find(|s| s.contains("region_new")).unwrap();
                assert!(creation.contains("region_id"));
                assert!(creation.contains("state"));
            }

            // Close the region
            let region_record = state.regions.get_mut(region.arena_index()).expect("region");
            region_record.begin_close(None);
            
            // Check for update
            {
                let spans = spans.lock().unwrap();
                let update = spans.iter().find(|s| s.contains("region_record") && s.contains("Closing")).expect("Should record Closing state");
            }

            region_record.begin_finalize();
            region_record.complete_close();
            
            // Check for final update
            {
                let spans = spans.lock().unwrap();
                let update = spans.iter().find(|s| s.contains("region_record") && s.contains("Closed")).expect("Should record Closed state");
            }
        });
    }

    #[test]
    fn verify_task_logs() {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let recorder = SpanRecorder { spans: logs.clone() };
        let subscriber = tracing_subscriber::registry().with(recorder);

        tracing::subscriber::with_default(subscriber, || {
            let mut state = RuntimeState::new();
            let region = state.create_root_region(Budget::INFINITE);
            
            // Create a task
            let _ = state.create_task(region, Budget::INFINITE, async { 42 });

            // Check for log
            {
                let logs = logs.lock().unwrap();
                let task_log = logs.iter().find(|s| s.contains("event") && s.contains("task created")).expect("Should record task creation log");
            }
        });
    }
}
