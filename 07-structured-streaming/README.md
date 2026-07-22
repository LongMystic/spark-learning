# Structured Streaming Mastery

## 🎯 Overview

Master Spark Structured Streaming for real-time ETL pipelines in your on-premise environment.

## 📚 Topics Covered

1. **Streaming Fundamentals**
   - Micro-batch processing
   - Event-time vs processing-time
   - Watermarks and late data

2. **Source & Sink Patterns**
   - Kafka integration
   - File streaming
   - Custom sources

3. **Stateful Processing**
   - State management
   - Checkpointing
   - Recovery mechanisms

4. **Performance Optimization**
   - Parallelism tuning
   - Backpressure handling
   - Resource allocation

5. **Production Patterns**
   - Exactly-once semantics
   - Monitoring
   - Error handling

## 🚀 Key Concepts

### Micro-batch Model
- Processes data in small batches
- Low latency (sub-second to seconds)
- Exactly-once guarantees possible

### Checkpointing
- Enables fault tolerance
- Allows query recovery
- Stores state and offsets

## 📖 Learning Path (Phase 5)

- [Day 30: Structured Streaming Fundamentals](day-30-streaming-fundamentals.md)
- [Day 31: Stateful Streaming, Watermarks, Kafka & Exactly-Once](day-31-stateful-streaming.md)

Exercises: [`exercises/streaming/`](../exercises/streaming/) (Kafka needs the `streaming` docker profile)

---

**Start**: [Day 30: Structured Streaming Fundamentals](day-30-streaming-fundamentals.md)

