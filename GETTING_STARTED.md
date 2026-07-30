# Getting Started Guide

## 🚀 Welcome to Your Spark Deep Dive Journey!

This guide will help you get started with your structured learning path.

## 📋 Prerequisites

You need **one** of these:
- **Kubernetes cluster (recommended to start)**: minikube (see [environment/README.md](environment/README.md)).
- **On-prem**: access to your Spark-on-Kubernetes cluster (kubectl) and the Spark UI.

Plus basic familiarity with Spark syntax and architecture (you have this! ✅).
See [environment/README.md](environment/README.md) to set up in ~5 minutes.

## 🗺️ Learning Path Overview

### Phase 1: Deep Fundamentals (Week 1)
**Goal**: Understand Spark internals at a deep level

- **Day 1**: Execution Model - How Spark runs your code
- **Day 2**: Catalyst Optimizer - How Spark optimizes queries
- **Day 3**: Memory Management - How Spark uses memory
- **Day 4**: Shuffle Mechanics - Understanding data movement
- **Day 5**: Partitioning - Data organization strategies
- **Day 6**: Join Algorithms - How joins work internally
- **Day 7**: Caching - When and how to cache data

### Phase 2: Performance Tuning (Week 2)
**Goal**: Optimize Spark jobs for production

- **Day 8**: Configuration tuning
- **Day 9**: Resource allocation
- **Day 10**: Data skew handling
- **Day 11**: Shuffle optimization
- **Day 12**: Join optimization
- **Day 13**: Memory optimization
- **Day 14**: Network optimization

### Phase 3: Troubleshooting (Week 3)
**Goal**: Debug and fix production issues

- **Day 15**: Common error patterns
- **Day 16**: OOM debugging
- **Day 17**: Task failure analysis
- **Day 18**: Shuffle error resolution (FetchFailed)
- **Day 19**: Serialization & UDF issues
- **Day 20**: Performance debugging
- **Day 21**: Production incident response

### Phase 4: Advanced Topics (Week 4)
**Goal**: Master advanced Spark features

- **Day 22**: Custom optimizations & Catalyst rules
- **Day 23**: Advanced SQL patterns
- **Day 24**: UDF/UDAF performance
- **Day 25**: Broadcast strategies & AQE
- **Day 26**: Bucketing techniques
- **Day 27**: Dynamic partition pruning
- **Day 28**: Cost-based optimization

### Phase 5: Production Patterns (Week 5+)
**Goal**: Apply knowledge to your stack

- **Day 29**: Spark Thrift Server optimization
- **Day 30-31**: Structured Streaming patterns (fundamentals, stateful/watermarks)
- **Day 32**: PySpark best practices & Zeppelin
- **Day 33-34**: Iceberg fundamentals & maintenance
- **Day 35**: Airflow orchestration integration
- **Day 36**: DBT-on-Spark integration
- **Day 37**: Superset query optimization
- **Day 38**: Large-scale ETL & CDC patterns
- **Day 39**: Architecture, multi-tenancy, HA & security
- **Day 40**: Cost/observability + capstone kickoff

## 📖 How to Use This Repository

### Daily Learning Routine

1. **Read the Day's Lesson** (30-45 minutes)
   - Located in numbered folders (e.g., `01-fundamentals/day-01-*.md`)
   - Take notes in `notes/` directory

2. **Complete Exercises** (30-60 minutes)
   - Located in `exercises/` directory
   - Run on your actual cluster when possible
   - Analyze results in Spark UI

3. **Experiment** (15-30 minutes)
   - Try variations of exercises
   - Apply to your actual use cases
   - Document findings

4. **Update Progress** (5 minutes)
   - Mark completed days in `PROGRESS.md`
   - Note key learnings

### Weekly Review

- Review completed lessons
- Identify areas needing more practice
- Apply learnings to real work
- Share insights with team

## 🎯 Learning Tips

### 1. Hands-On Practice
- **Don't just read** - run the code!
- Use your actual cluster and data
- Experiment with different configurations

### 2. Spark UI is Your Friend
- Always check Spark UI after running queries
- Analyze DAGs, stages, and tasks
- Monitor metrics and identify bottlenecks

### 3. Take Notes
- Document "aha!" moments
- Record environment-specific findings
- Track performance improvements

### 4. Apply to Real Work
- Try optimizations on actual jobs
- Share learnings with your team
- Measure before/after performance

### 5. Be Patient
- Some concepts take time to sink in
- Revisit lessons if needed
- Practice makes perfect

## 🔧 Setup Your Environment

Full instructions: [environment/README.md](environment/README.md). In short:

### 1. Install & generate data

Follow the instructions in `environment/README.md` to run the data generation job on your Kubernetes cluster, which will write the sample tables to `s3a://warehouse/data`.

### 2. Exercises use a shared SparkSession

You never hand-build a session — exercises import it, so the same code runs smoothly on your cluster:

```python
from common.spark_session import get_spark, read_table
spark = get_spark("Learning Exercise")
txns  = read_table(spark, "transactions")  # reads the generated sample data
```



### 3. Access the Spark UI

- Docker cluster: master `http://localhost:8080`, history `http://localhost:18080`
- On K8S: `kubectl -n spark-jobs get pods` to find the driver, then `kubectl -n spark-jobs port-forward <driver-pod> 4040` for the live UI (and port-forward the History Server `:18080` for finished apps)

## 📊 Tracking Progress

### Update PROGRESS.md
- Mark completed days
- Note key metrics
- Track improvements

### Document Learnings
- Use `notes/` directory
- Create files by topic or date
- Include code snippets and insights

## 🚨 Common Challenges

### "I don't have time"
- **Solution**: Even 30 minutes daily adds up
- Focus on one concept at a time
- Apply immediately to real work

### "Concepts are too complex"
- **Solution**: Break down into smaller pieces
- Re-read and practice more
- Experiment with code examples

### "Can't access Spark UI"
- **Solution**: Ask your cluster admin
- Use `explain()` to see plans
- Check logs for insights

## 🎓 Success Metrics

Track your progress (full competency list in [assessments/mastery-checklist.md](assessments/mastery-checklist.md)):

- [ ] Completed all 40 days
- [ ] Passed all 5 phase assessments (`assessments/`)
- [ ] Optimized at least 5 production jobs
- [ ] Fixed at least 3 production issues
- [ ] Shared knowledge with team
- [ ] Improved job performance by 20%+

## 🔗 Next Steps

1. **Start with Day 1**: Read `01-fundamentals/day-01-execution-model.md`
2. **Run Exercise 1**: Complete `exercises/fundamentals/exercise-01-dag-analysis.py`
3. **Check Spark UI**: Analyze the DAG visualization
4. **Update Progress**: Mark Day 1 complete in `PROGRESS.md`

## 💬 Questions?

- Review the lesson again
- Experiment with code
- Check Spark documentation
- Discuss with your team

---

**Ready to begin? Start with Day 1! 🚀**

Remember: This is a marathon, not a sprint. Consistency beats intensity.

Good luck on your Spark mastery journey! 🎉

