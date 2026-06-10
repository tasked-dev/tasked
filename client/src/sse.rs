//! Server-Sent Events (SSE) subscription for flow events.

use crate::error::TaskedError;
use crate::{TaskedClient, encode_path};
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;

/// An SSE event from the flow events stream.
#[derive(Debug, Clone, PartialEq)]
pub enum FlowEvent {
    /// A task changed state.
    TaskState {
        task_id: String,
        state: String,
        output: Option<serde_json::Value>,
        error: Option<String>,
        started_at: Option<String>,
        completed_at: Option<String>,
    },
    /// A task's output was updated (while running).
    TaskOutput {
        task_id: String,
        output: serde_json::Value,
    },
    /// The flow reached a terminal state.
    FlowComplete {
        flow_id: String,
        state: String,
        task_count: usize,
        tasks_succeeded: usize,
        tasks_failed: usize,
    },
    /// A keep-alive ping.
    Ping,
    /// An unknown or unparseable event.
    Unknown { event: String, data: String },
}

#[derive(Deserialize)]
struct TaskStateData {
    task_id: String,
    state: String,
    output: Option<serde_json::Value>,
    error: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
}

#[derive(Deserialize)]
struct TaskOutputData {
    task_id: String,
    output: serde_json::Value,
}

#[derive(Deserialize)]
struct FlowCompleteData {
    flow_id: String,
    state: String,
    task_count: usize,
    tasks_succeeded: usize,
    tasks_failed: usize,
}

/// Incremental SSE parser.
///
/// Accumulates raw bytes and emits one [`FlowEvent`] per complete SSE event,
/// where events are delimited by a blank line (`\n\n`, tolerating `\r\n`
/// line endings). UTF-8 is validated only at complete-event granularity, so
/// multi-byte characters split across TCP chunks are handled correctly.
#[derive(Debug, Default)]
struct SseParser {
    buf: Vec<u8>,
}

impl SseParser {
    fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk of bytes, returning all events completed by it.
    fn push(&mut self, chunk: &[u8]) -> Vec<Result<FlowEvent, TaskedError>> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some((event_end, rest_start)) = find_event_boundary(&self.buf) {
            let event_bytes: Vec<u8> = self.buf.drain(..rest_start).take(event_end).collect();
            match std::str::from_utf8(&event_bytes) {
                Ok(text) => {
                    if let Some(event) = parse_event(text) {
                        out.push(Ok(event));
                    }
                }
                Err(e) => out.push(Err(TaskedError::InvalidResponse(format!(
                    "invalid UTF-8 in SSE event: {e}"
                )))),
            }
        }
        out
    }
}

/// Find the end of the first complete event in `buf`.
///
/// Returns `(event_end, rest_start)` where `buf[..event_end]` is the event
/// text (without the blank-line delimiter) and `buf[rest_start..]` is the
/// remainder. Recognizes `\n\n` and `\n\r\n` delimiters; a `\r\n\r\n`
/// sequence matches the latter, leaving a trailing `\r` that line parsing
/// strips.
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    for (i, &b) in buf.iter().enumerate() {
        if b != b'\n' {
            continue;
        }
        if buf.get(i + 1) == Some(&b'\n') {
            return Some((i, i + 2));
        }
        if buf.get(i + 1) == Some(&b'\r') && buf.get(i + 2) == Some(&b'\n') {
            return Some((i, i + 3));
        }
    }
    None
}

/// Parse the text of one complete SSE event.
///
/// Comment lines (starting with `:`) within an event are ignored; an event
/// consisting solely of comments (the server's keep-alive `:ping`) yields
/// [`FlowEvent::Ping`]. Multiple `data:` lines are joined with `\n` per the
/// SSE specification. Returns `None` for fully empty events.
fn parse_event(text: &str) -> Option<FlowEvent> {
    let mut event_type = String::new();
    let mut data_lines: Vec<&str> = Vec::new();
    let mut saw_comment = false;

    for line in text.lines() {
        // `str::lines` strips `\n` but a `\r\n\r\n` delimiter can leave a
        // trailing `\r` on the final line.
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        if line.starts_with(':') {
            saw_comment = true;
            continue;
        }
        let (field, value) = match line.split_once(':') {
            // Per the SSE spec, a single leading space in the value is stripped.
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => event_type = value.to_string(),
            "data" => data_lines.push(value),
            // `id:`, `retry:`, and unknown fields are ignored.
            _ => {}
        }
    }

    if event_type.is_empty() && data_lines.is_empty() {
        return saw_comment.then_some(FlowEvent::Ping);
    }

    let data = data_lines.join("\n");
    Some(dispatch_event(event_type, data))
}

/// Map a parsed (event type, data) pair onto a [`FlowEvent`].
fn dispatch_event(event_type: String, data: String) -> FlowEvent {
    match event_type.as_str() {
        "task_state" => match serde_json::from_str::<TaskStateData>(&data) {
            Ok(d) => FlowEvent::TaskState {
                task_id: d.task_id,
                state: d.state,
                output: d.output,
                error: d.error,
                started_at: d.started_at,
                completed_at: d.completed_at,
            },
            Err(_) => FlowEvent::Unknown {
                event: event_type,
                data,
            },
        },
        "task_output" => match serde_json::from_str::<TaskOutputData>(&data) {
            Ok(d) => FlowEvent::TaskOutput {
                task_id: d.task_id,
                output: d.output,
            },
            Err(_) => FlowEvent::Unknown {
                event: event_type,
                data,
            },
        },
        "flow_complete" => match serde_json::from_str::<FlowCompleteData>(&data) {
            Ok(d) => FlowEvent::FlowComplete {
                flow_id: d.flow_id,
                state: d.state,
                task_count: d.task_count,
                tasks_succeeded: d.tasks_succeeded,
                tasks_failed: d.tasks_failed,
            },
            Err(_) => FlowEvent::Unknown {
                event: event_type,
                data,
            },
        },
        _ => FlowEvent::Unknown {
            event: event_type,
            data,
        },
    }
}

impl TaskedClient {
    /// Subscribe to SSE events for a flow.
    ///
    /// Returns a stream of [`FlowEvent`]s. The stream ends when the flow
    /// reaches a terminal state or the connection is closed. This request
    /// uses a dedicated client without a total request timeout, since the
    /// stream is long-lived.
    pub async fn flow_events(
        &self,
        flow_id: &str,
    ) -> Result<impl Stream<Item = Result<FlowEvent, TaskedError>>, TaskedError> {
        let url = format!(
            "{}/api/v1/flows/{}/events",
            self.base_url,
            encode_path(flow_id)
        );
        let resp = self.apply_auth(self.sse_client.get(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(crate::error::parse_error(resp).await);
        }

        let mut parser = SseParser::new();
        let stream = resp
            .bytes_stream()
            .map(move |chunk| {
                let events = match chunk {
                    Ok(bytes) => parser.push(&bytes),
                    Err(e) => vec![Err(TaskedError::Http(e))],
                };
                futures::stream::iter(events)
            })
            .flatten();

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_ok(parser: &mut SseParser, chunk: &[u8]) -> Vec<FlowEvent> {
        parser
            .push(chunk)
            .into_iter()
            .map(|r| r.expect("event should parse"))
            .collect()
    }

    fn task_output(id: &str, value: i64) -> FlowEvent {
        FlowEvent::TaskOutput {
            task_id: id.to_string(),
            output: serde_json::json!(value),
        }
    }

    #[test]
    fn event_split_across_chunks() {
        let mut p = SseParser::new();
        assert!(push_ok(&mut p, b"event: task_output\nda").is_empty());
        assert!(push_ok(&mut p, b"ta: {\"task_id\":\"t1\",").is_empty());
        let events = push_ok(&mut p, b"\"output\":1}\n\n");
        assert_eq!(events, vec![task_output("t1", 1)]);
    }

    #[test]
    fn two_events_in_one_chunk() {
        let mut p = SseParser::new();
        let events = push_ok(
            &mut p,
            b"event: task_output\ndata: {\"task_id\":\"a\",\"output\":1}\n\n\
              event: task_output\ndata: {\"task_id\":\"b\",\"output\":2}\n\n",
        );
        assert_eq!(events, vec![task_output("a", 1), task_output("b", 2)]);
    }

    #[test]
    fn ping_comment_interleaved_with_real_event_in_one_chunk() {
        // Standalone comment-only event followed by a real event in one chunk:
        // the comment must not swallow the real event.
        let mut p = SseParser::new();
        let events = push_ok(
            &mut p,
            b":ping\n\nevent: task_output\ndata: {\"task_id\":\"t1\",\"output\":1}\n\n",
        );
        assert_eq!(events, vec![FlowEvent::Ping, task_output("t1", 1)]);

        // A comment line inside an event is ignored; the event still parses.
        let events = push_ok(
            &mut p,
            b"event: task_output\n:ping\ndata: {\"task_id\":\"t2\",\"output\":2}\n\n",
        );
        assert_eq!(events, vec![task_output("t2", 2)]);
    }

    #[test]
    fn multibyte_utf8_split_across_chunks() {
        // "é" is 0xC3 0xA9; split the event between the two bytes.
        let raw = "event: task_output\ndata: {\"task_id\":\"café\",\"output\":1}\n\n".as_bytes();
        let split = raw
            .windows(2)
            .position(|w| w == [0xC3, 0xA9])
            .expect("multi-byte char present")
            + 1;
        let mut p = SseParser::new();
        assert!(push_ok(&mut p, &raw[..split]).is_empty());
        let events = push_ok(&mut p, &raw[split..]);
        assert_eq!(events, vec![task_output("café", 1)]);
    }

    #[test]
    fn crlf_line_endings() {
        let mut p = SseParser::new();
        let events = push_ok(
            &mut p,
            b"event: task_output\r\ndata: {\"task_id\":\"t1\",\"output\":1}\r\n\r\n",
        );
        assert_eq!(events, vec![task_output("t1", 1)]);
    }

    #[test]
    fn multiple_data_lines_join_with_newline() {
        let mut p = SseParser::new();
        let events = push_ok(&mut p, b"event: custom\ndata: line1\ndata: line2\n\n");
        assert_eq!(
            events,
            vec![FlowEvent::Unknown {
                event: "custom".to_string(),
                data: "line1\nline2".to_string(),
            }]
        );
    }

    #[test]
    fn flow_complete_event() {
        let mut p = SseParser::new();
        let events = push_ok(
            &mut p,
            b"event: flow_complete\ndata: {\"flow_id\":\"f1\",\"state\":\"succeeded\",\
              \"task_count\":3,\"tasks_succeeded\":3,\"tasks_failed\":0}\n\n",
        );
        assert_eq!(
            events,
            vec![FlowEvent::FlowComplete {
                flow_id: "f1".to_string(),
                state: "succeeded".to_string(),
                task_count: 3,
                tasks_succeeded: 3,
                tasks_failed: 0,
            }]
        );
    }

    #[test]
    fn invalid_utf8_yields_error_not_corruption() {
        let mut p = SseParser::new();
        let mut chunk = b"event: task_output\ndata: ".to_vec();
        chunk.extend_from_slice(&[0xC3]); // dangling lead byte
        chunk.extend_from_slice(b"\n\n");
        let results = p.push(&chunk);
        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0],
            Err(TaskedError::InvalidResponse(_))
        ));
    }
}
