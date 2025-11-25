# Real-Time Cyberbullying Detection Pipeline

This project demonstrates a full real-time data engineering pipeline for detecting cyberbullying content from social platforms (e.g., YouTube, Twitter, Reddit).

## Overview

The goal is to **collect, stream, process, classify, and store social media content in real-time** to detect toxic or harassing messages.  

The system is fully containerized using **Docker Compose** and includes:

- **Ingestion service** → connects to social media APIs and streams data to Kafka  
- **Kafka cluster** → high-throughput message broker for real-time streaming  
- **Consumer service** → reads from Kafka, applies AI cyberbullying detection, writes results to MinIO  
- **MinIO** → object storage for raw and processed data  
- **Data processing & treatment** → cleans and transforms data for analytics  
- **Dashboard / Monitoring** → real-time visualization of cyberbullying trends
