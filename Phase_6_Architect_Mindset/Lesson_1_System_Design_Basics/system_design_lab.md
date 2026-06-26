# system_design_lab.md
# Phase 6, Lesson 1: System Design Basics
# Goal: Design a scalable data ingestion system.

## 🏗️ Phase 1: Absolute Foundations (Beginner)
**The Scenario:** You need to ingest 1 million sales records daily from a REST API into a Data Warehouse. 
**Exercise:** Draw (or list) the 3 main components of this system:
1. **Source:** (The API)
2. **Compute:** (Spark/Python script)
3. **Storage:** (S3/Data Lake)

## 🚀 Phase 2: Intermediate (Developer)
**Adding Complexity:** What if the API is slow and you need to process data as it arrives?
**Exercise:** Update your design to include an **Event Bus** (e.g., Kafka or Pub/Sub). This allows you to decouple the "Collector" from the "Processor". 

## 🏛️ Phase 3: Architect (Professional)
**Thinking about Failures:**
1. What if 'Job A' fails halfway? (Idempotency)
2. How do you keep track of 'Total Sales' if some data arrive 2 hours late? (Watermarking)
3. How do you alert the team? (Monitoring/Logging)

**The Design Challenge:** 
"Create a Mermaid diagram for a system that can handle 10,000 events per second with 99.9% reliability."

---

## 🏛️ Architect's Tip:
"A good system design is not about 'never failing'. It's about 'failing gracefully'. Design your pipelines to be RESTARABLE from any point. If you have to manually clean up data after a failure, you've failed as an architect."
