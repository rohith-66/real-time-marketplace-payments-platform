# Event Model

This project models marketplace payment lifecycle events for a platform similar to DoorDash, Uber Eats, or Instacart.

## Event Lifecycle

The pipeline will simulate these payment-related events:

- payment_authorized
- payment_captured
- refund_issued
- payout_sent
- payment_failed

## Business Meaning

Each order on the marketplace can emit one or more payment lifecycle events. These events are processed downstream to support:

- platform revenue reporting
- seller payout analytics
- payment success and failure tracking
- city-level transaction analysis

## Core Entities

- customer
- seller
- order
- payment event
- marketplace platform

## Notes

This schema acts as the data contract for the synthetic event producer and downstream Spark streaming pipeline.