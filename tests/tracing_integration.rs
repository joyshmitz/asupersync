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
                spans.push(format!("region_new: {:?}", attrs.metadata().fields()));
            }
        }

        fn on_record(&self, _id: &tracing::Id, _values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
             // In a real test we'd check if this is the region span
             // For now just logging strictly region name in on_new_span is enough to prove it exists
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
            }

            // Close the region
            let region_record = state.regions.get_mut(region.arena_index()).expect("region");
            region_record.begin_close(None);
            region_record.begin_finalize();
            region_record.complete_close();
        });
    }
}
