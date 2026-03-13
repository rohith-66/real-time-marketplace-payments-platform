# Real-Time Marketplace Payments Processing Platform

A real-time data engineering project that processes marketplace payment lifecycle events using Kafka, Spark Structured Streaming, Airflow, and analytics-ready transformations.

## Business Context

This platform simulates a marketplace business similar to DoorDash, Uber Eats, or Instacart, where customer orders generate payment events such as authorization, capture, refund, and seller payout.

## Planned Architecture

Payment Event Generator -> Kafka -> Spark Structured Streaming -> Bronze -> Silver -> Gold -> Airflow Orchestration

## Project Goals

- Build a real-time event-driven data pipeline
- Process marketplace payment lifecycle events
- Create analytics-ready metrics for sellers, cities, and platform revenue
- Demonstrate batch + streaming data engineering skills

## Status

Project setup in progress.