# Appendix A — Real event histories

These histories were captured from this project running against a Temporal
dev server (CLI 1.5.1, server 1.29.1, Python SDK 1.34.0) with the offline
`fake:retail` model. Nothing has been edited except removing payloads for
readability. Each line is: event ID, event type, and the activity, workflow,
signal, update, marker or timer it refers to.

Regenerate them yourself by running the scenarios in `tests/` and then
`temporal workflow show -w <workflow-id>`.

Things to look for:

* **Order happy path** — the rhythm of `WORKFLOW_TASK_*` triplets between
  each command; the patch marker at #61 and the `TemporalChangeVersion`
  upsert at #62; four `shipment_progress` signals from the child; the durable
  timer at #117.
* **Fraud graph** — only activity-nodes appear (`fraud-risk.collect_signals`,
  `fraud-risk.llm_review`). The workflow-side nodes leave no trace. The
  clear-cut case never calls the LLM.
* **Shipment child** — `ACTIVITY_TASK_SCHEDULED` at #5 but
  `ACTIVITY_TASK_STARTED` only at #18: the started event is written when the
  attempt finishes. Three carrier signals arrived in between and were buffered.
* **Declined card** — `ACTIVITY_TASK_FAILED` at #44 (one attempt; the error is
  non-retryable), then `refund_payment` and `release_inventory` compensations
  in reverse order, then a normal `WORKFLOW_EXECUTION_COMPLETED`: a declined
  card is a business outcome, not a workflow failure.
* **Support Deep Agent** — the chat update (#5) produces two model calls
  before the first tool (the first one planned with `write_todos`, which runs
  in-workflow), the update completes at #45 with the refund parked for
  approval, and the `decide` update (#49) resumes by running `issue_refund`.
  The idle timer is cancelled and re-armed on activity.
* **Operator-cancelled order** — `WORKFLOW_EXECUTION_CANCEL_REQUESTED` at #43
  while the review timer was running, the workflow's `CancelledError` handler
  cancels the timer and records the final status, and the execution ends as
  `WORKFLOW_EXECUTION_CANCELED`.

```text

### Order happy path (order-ORD-1001)
   1  WORKFLOW_EXECUTION_STARTED                       OrderFulfillmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   6  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   7  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
   8  ACTIVITY_TASK_STARTED                            
   9  ACTIVITY_TASK_COMPLETED                          
  10  WORKFLOW_TASK_SCHEDULED                          
  11  WORKFLOW_TASK_STARTED                            
  12  WORKFLOW_TASK_COMPLETED                          
  13  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  14  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  15  ACTIVITY_TASK_STARTED                            
  16  ACTIVITY_TASK_COMPLETED                          
  17  WORKFLOW_TASK_SCHEDULED                          
  18  WORKFLOW_TASK_STARTED                            
  19  WORKFLOW_TASK_COMPLETED                          
  20  START_CHILD_WORKFLOW_EXECUTION_INITIATED         FraudAssessmentWorkflow
  21  CHILD_WORKFLOW_EXECUTION_STARTED                 FraudAssessmentWorkflow
  22  WORKFLOW_TASK_SCHEDULED                          
  23  WORKFLOW_TASK_STARTED                            
  24  WORKFLOW_TASK_COMPLETED                          
  25  CHILD_WORKFLOW_EXECUTION_COMPLETED               FraudAssessmentWorkflow
  26  WORKFLOW_TASK_SCHEDULED                          
  27  WORKFLOW_TASK_STARTED                            
  28  WORKFLOW_TASK_COMPLETED                          
  29  ACTIVITY_TASK_SCHEDULED                          reserve_inventory
  30  ACTIVITY_TASK_STARTED                            
  31  ACTIVITY_TASK_COMPLETED                          
  32  WORKFLOW_TASK_SCHEDULED                          
  33  WORKFLOW_TASK_STARTED                            
  34  WORKFLOW_TASK_COMPLETED                          
  35  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  36  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  37  ACTIVITY_TASK_STARTED                            
  38  ACTIVITY_TASK_COMPLETED                          
  39  WORKFLOW_TASK_SCHEDULED                          
  40  WORKFLOW_TASK_STARTED                            
  41  WORKFLOW_TASK_COMPLETED                          
  42  ACTIVITY_TASK_SCHEDULED                          charge_payment
  43  ACTIVITY_TASK_STARTED                            
  44  ACTIVITY_TASK_COMPLETED                          
  45  WORKFLOW_TASK_SCHEDULED                          
  46  WORKFLOW_TASK_STARTED                            
  47  WORKFLOW_TASK_COMPLETED                          
  48  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  49  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  50  ACTIVITY_TASK_STARTED                            
  51  ACTIVITY_TASK_COMPLETED                          
  52  WORKFLOW_TASK_SCHEDULED                          
  53  WORKFLOW_TASK_STARTED                            
  54  WORKFLOW_TASK_COMPLETED                          
  55  ACTIVITY_TASK_SCHEDULED                          send_notification
  56  ACTIVITY_TASK_STARTED                            
  57  ACTIVITY_TASK_COMPLETED                          
  58  WORKFLOW_TASK_SCHEDULED                          
  59  WORKFLOW_TASK_STARTED                            
  60  WORKFLOW_TASK_COMPLETED                          
  61  MARKER_RECORDED                                  core_patch
  62  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  63  ACTIVITY_TASK_SCHEDULED                          award_loyalty_points
  64  ACTIVITY_TASK_STARTED                            
  65  ACTIVITY_TASK_COMPLETED                          
  66  WORKFLOW_TASK_SCHEDULED                          
  67  WORKFLOW_TASK_STARTED                            
  68  WORKFLOW_TASK_COMPLETED                          
  69  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  70  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  71  ACTIVITY_TASK_STARTED                            
  72  ACTIVITY_TASK_COMPLETED                          
  73  WORKFLOW_TASK_SCHEDULED                          
  74  WORKFLOW_TASK_STARTED                            
  75  WORKFLOW_TASK_COMPLETED                          
  76  START_CHILD_WORKFLOW_EXECUTION_INITIATED         ShipmentWorkflow
  77  CHILD_WORKFLOW_EXECUTION_STARTED                 ShipmentWorkflow
  78  WORKFLOW_TASK_SCHEDULED                          
  79  WORKFLOW_TASK_STARTED                            
  80  WORKFLOW_TASK_COMPLETED                          
  81  WORKFLOW_EXECUTION_SIGNALED                      shipment_progress
  82  WORKFLOW_TASK_SCHEDULED                          
  83  WORKFLOW_TASK_STARTED                            
  84  WORKFLOW_TASK_COMPLETED                          
  85  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  86  WORKFLOW_EXECUTION_SIGNALED                      shipment_progress
  87  WORKFLOW_TASK_SCHEDULED                          
  88  WORKFLOW_TASK_STARTED                            
  89  WORKFLOW_TASK_COMPLETED                          
  90  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  91  WORKFLOW_EXECUTION_SIGNALED                      shipment_progress
  92  WORKFLOW_TASK_SCHEDULED                          
  93  WORKFLOW_TASK_STARTED                            
  94  WORKFLOW_TASK_COMPLETED                          
  95  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  96  WORKFLOW_EXECUTION_SIGNALED                      shipment_progress
  97  WORKFLOW_TASK_SCHEDULED                          
  98  WORKFLOW_TASK_STARTED                            
  99  WORKFLOW_TASK_COMPLETED                          
 100  CHILD_WORKFLOW_EXECUTION_COMPLETED               ShipmentWorkflow
 101  WORKFLOW_TASK_SCHEDULED                          
 102  WORKFLOW_TASK_STARTED                            
 103  WORKFLOW_TASK_COMPLETED                          
 104  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
 105  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
 106  ACTIVITY_TASK_STARTED                            
 107  ACTIVITY_TASK_COMPLETED                          
 108  WORKFLOW_TASK_SCHEDULED                          
 109  WORKFLOW_TASK_STARTED                            
 110  WORKFLOW_TASK_COMPLETED                          
 111  ACTIVITY_TASK_SCHEDULED                          send_notification
 112  ACTIVITY_TASK_STARTED                            
 113  ACTIVITY_TASK_COMPLETED                          
 114  WORKFLOW_TASK_SCHEDULED                          
 115  WORKFLOW_TASK_STARTED                            
 116  WORKFLOW_TASK_COMPLETED                          
 117  TIMER_STARTED                                    timer 1s
 118  TIMER_FIRED                                      
 119  WORKFLOW_TASK_SCHEDULED                          
 120  WORKFLOW_TASK_STARTED                            
 121  WORKFLOW_TASK_COMPLETED                          
 122  ACTIVITY_TASK_SCHEDULED                          send_notification
 123  ACTIVITY_TASK_STARTED                            
 124  ACTIVITY_TASK_COMPLETED                          
 125  WORKFLOW_TASK_SCHEDULED                          
 126  WORKFLOW_TASK_STARTED                            
 127  WORKFLOW_TASK_COMPLETED                          
 128  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
 129  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
 130  ACTIVITY_TASK_STARTED                            
 131  ACTIVITY_TASK_COMPLETED                          
 132  WORKFLOW_TASK_SCHEDULED                          
 133  WORKFLOW_TASK_STARTED                            
 134  WORKFLOW_TASK_COMPLETED                          
 135  WORKFLOW_EXECUTION_COMPLETED                     

### Fraud graph (approve, no LLM) (fraud-ORD-1001)
   1  WORKFLOW_EXECUTION_STARTED                       FraudAssessmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  ACTIVITY_TASK_SCHEDULED                          fraud-risk.collect_signals
   6  ACTIVITY_TASK_STARTED                            
   7  ACTIVITY_TASK_COMPLETED                          
   8  WORKFLOW_TASK_SCHEDULED                          
   9  WORKFLOW_TASK_STARTED                            
  10  WORKFLOW_TASK_COMPLETED                          
  11  WORKFLOW_EXECUTION_COMPLETED                     

### Shipment child (shipment-ORD-1001)
   1  WORKFLOW_EXECUTION_STARTED                       ShipmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  ACTIVITY_TASK_SCHEDULED                          create_shipping_label
   6  WORKFLOW_EXECUTION_SIGNALED                      carrier_event
   7  WORKFLOW_TASK_SCHEDULED                          
   8  WORKFLOW_TASK_STARTED                            
   9  WORKFLOW_TASK_COMPLETED                          
  10  WORKFLOW_EXECUTION_SIGNALED                      carrier_event
  11  WORKFLOW_EXECUTION_SIGNALED                      carrier_event
  12  WORKFLOW_TASK_SCHEDULED                          
  13  WORKFLOW_TASK_STARTED                            
  14  WORKFLOW_TASK_COMPLETED                          
  15  ACTIVITY_TASK_STARTED                            
  16  ACTIVITY_TASK_COMPLETED                          
  17  WORKFLOW_TASK_SCHEDULED                          
  18  WORKFLOW_TASK_STARTED                            
  19  WORKFLOW_TASK_COMPLETED                          
  20  SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED     shipment_progress
  21  EXTERNAL_WORKFLOW_EXECUTION_SIGNALED             
  22  WORKFLOW_TASK_SCHEDULED                          
  23  WORKFLOW_TASK_STARTED                            
  24  WORKFLOW_TASK_COMPLETED                          
  25  SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED     shipment_progress
  26  EXTERNAL_WORKFLOW_EXECUTION_SIGNALED             
  27  WORKFLOW_TASK_SCHEDULED                          
  28  WORKFLOW_TASK_STARTED                            
  29  WORKFLOW_TASK_COMPLETED                          
  30  SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED     shipment_progress
  31  EXTERNAL_WORKFLOW_EXECUTION_SIGNALED             
  32  WORKFLOW_TASK_SCHEDULED                          
  33  WORKFLOW_TASK_STARTED                            
  34  WORKFLOW_TASK_COMPLETED                          
  35  SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED     shipment_progress
  36  EXTERNAL_WORKFLOW_EXECUTION_SIGNALED             
  37  WORKFLOW_TASK_SCHEDULED                          
  38  WORKFLOW_TASK_STARTED                            
  39  WORKFLOW_TASK_COMPLETED                          
  40  WORKFLOW_EXECUTION_COMPLETED                     

### Fraud graph (grey zone -> LLM) (fraud-ORD-1002)
   1  WORKFLOW_EXECUTION_STARTED                       FraudAssessmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  ACTIVITY_TASK_SCHEDULED                          fraud-risk.collect_signals
   6  ACTIVITY_TASK_STARTED                            
   7  ACTIVITY_TASK_COMPLETED                          
   8  WORKFLOW_TASK_SCHEDULED                          
   9  WORKFLOW_TASK_STARTED                            
  10  WORKFLOW_TASK_COMPLETED                          
  11  ACTIVITY_TASK_SCHEDULED                          fraud-risk.llm_review
  12  ACTIVITY_TASK_STARTED                            
  13  ACTIVITY_TASK_COMPLETED                          
  14  WORKFLOW_TASK_SCHEDULED                          
  15  WORKFLOW_TASK_STARTED                            
  16  WORKFLOW_TASK_COMPLETED                          
  17  WORKFLOW_EXECUTION_COMPLETED                     

### Declined card saga (order-ORD-1003)
   1  WORKFLOW_EXECUTION_STARTED                       OrderFulfillmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   6  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   7  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
   8  ACTIVITY_TASK_STARTED                            
   9  ACTIVITY_TASK_COMPLETED                          
  10  WORKFLOW_TASK_SCHEDULED                          
  11  WORKFLOW_TASK_STARTED                            
  12  WORKFLOW_TASK_COMPLETED                          
  13  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  14  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  15  ACTIVITY_TASK_STARTED                            
  16  ACTIVITY_TASK_COMPLETED                          
  17  WORKFLOW_TASK_SCHEDULED                          
  18  WORKFLOW_TASK_STARTED                            
  19  WORKFLOW_TASK_COMPLETED                          
  20  START_CHILD_WORKFLOW_EXECUTION_INITIATED         FraudAssessmentWorkflow
  21  CHILD_WORKFLOW_EXECUTION_STARTED                 FraudAssessmentWorkflow
  22  WORKFLOW_TASK_SCHEDULED                          
  23  WORKFLOW_TASK_STARTED                            
  24  WORKFLOW_TASK_COMPLETED                          
  25  CHILD_WORKFLOW_EXECUTION_COMPLETED               FraudAssessmentWorkflow
  26  WORKFLOW_TASK_SCHEDULED                          
  27  WORKFLOW_TASK_STARTED                            
  28  WORKFLOW_TASK_COMPLETED                          
  29  ACTIVITY_TASK_SCHEDULED                          reserve_inventory
  30  ACTIVITY_TASK_STARTED                            
  31  ACTIVITY_TASK_COMPLETED                          
  32  WORKFLOW_TASK_SCHEDULED                          
  33  WORKFLOW_TASK_STARTED                            
  34  WORKFLOW_TASK_COMPLETED                          
  35  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  36  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  37  ACTIVITY_TASK_STARTED                            
  38  ACTIVITY_TASK_COMPLETED                          
  39  WORKFLOW_TASK_SCHEDULED                          
  40  WORKFLOW_TASK_STARTED                            
  41  WORKFLOW_TASK_COMPLETED                          
  42  ACTIVITY_TASK_SCHEDULED                          charge_payment
  43  ACTIVITY_TASK_STARTED                            
  44  ACTIVITY_TASK_FAILED                             
  45  WORKFLOW_TASK_SCHEDULED                          
  46  WORKFLOW_TASK_STARTED                            
  47  WORKFLOW_TASK_COMPLETED                          
  48  ACTIVITY_TASK_SCHEDULED                          refund_payment
  49  ACTIVITY_TASK_STARTED                            
  50  ACTIVITY_TASK_COMPLETED                          
  51  WORKFLOW_TASK_SCHEDULED                          
  52  WORKFLOW_TASK_STARTED                            
  53  WORKFLOW_TASK_COMPLETED                          
  54  ACTIVITY_TASK_SCHEDULED                          release_inventory
  55  ACTIVITY_TASK_STARTED                            
  56  ACTIVITY_TASK_COMPLETED                          
  57  WORKFLOW_TASK_SCHEDULED                          
  58  WORKFLOW_TASK_STARTED                            
  59  WORKFLOW_TASK_COMPLETED                          
  60  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  61  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  62  ACTIVITY_TASK_STARTED                            
  63  ACTIVITY_TASK_COMPLETED                          
  64  WORKFLOW_TASK_SCHEDULED                          
  65  WORKFLOW_TASK_STARTED                            
  66  WORKFLOW_TASK_COMPLETED                          
  67  ACTIVITY_TASK_SCHEDULED                          send_notification
  68  ACTIVITY_TASK_STARTED                            
  69  ACTIVITY_TASK_COMPLETED                          
  70  WORKFLOW_TASK_SCHEDULED                          
  71  WORKFLOW_TASK_STARTED                            
  72  WORKFLOW_TASK_COMPLETED                          
  73  WORKFLOW_EXECUTION_COMPLETED                     

### Support Deep Agent refund with approval (support-demo)
   1  WORKFLOW_EXECUTION_STARTED                       SupportAgentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  WORKFLOW_EXECUTION_UPDATE_ACCEPTED               chat
   6  MARKER_RECORDED                                  core_patch
   7  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   8  ACTIVITY_TASK_SCHEDULED                          deepagents.invoke_model
   9  TIMER_STARTED                                    timer 1800s
  10  ACTIVITY_TASK_STARTED                            
  11  ACTIVITY_TASK_COMPLETED                          
  12  WORKFLOW_TASK_SCHEDULED                          
  13  WORKFLOW_TASK_STARTED                            
  14  WORKFLOW_TASK_COMPLETED                          
  15  ACTIVITY_TASK_SCHEDULED                          deepagents.invoke_model
  16  ACTIVITY_TASK_STARTED                            
  17  ACTIVITY_TASK_COMPLETED                          
  18  WORKFLOW_TASK_SCHEDULED                          
  19  WORKFLOW_TASK_STARTED                            
  20  WORKFLOW_TASK_COMPLETED                          
  21  ACTIVITY_TASK_SCHEDULED                          check_return_eligibility
  22  ACTIVITY_TASK_STARTED                            
  23  ACTIVITY_TASK_COMPLETED                          
  24  WORKFLOW_TASK_SCHEDULED                          
  25  WORKFLOW_TASK_STARTED                            
  26  WORKFLOW_TASK_COMPLETED                          
  27  ACTIVITY_TASK_SCHEDULED                          deepagents.invoke_model
  28  ACTIVITY_TASK_STARTED                            
  29  ACTIVITY_TASK_COMPLETED                          
  30  WORKFLOW_TASK_SCHEDULED                          
  31  WORKFLOW_TASK_STARTED                            
  32  WORKFLOW_TASK_COMPLETED                          
  33  ACTIVITY_TASK_SCHEDULED                          create_return_label
  34  ACTIVITY_TASK_STARTED                            
  35  ACTIVITY_TASK_COMPLETED                          
  36  WORKFLOW_TASK_SCHEDULED                          
  37  WORKFLOW_TASK_STARTED                            
  38  WORKFLOW_TASK_COMPLETED                          
  39  ACTIVITY_TASK_SCHEDULED                          deepagents.invoke_model
  40  ACTIVITY_TASK_STARTED                            
  41  ACTIVITY_TASK_COMPLETED                          
  42  WORKFLOW_TASK_SCHEDULED                          
  43  WORKFLOW_TASK_STARTED                            
  44  WORKFLOW_TASK_COMPLETED                          
  45  WORKFLOW_EXECUTION_UPDATE_COMPLETED              
  46  WORKFLOW_TASK_SCHEDULED                          
  47  WORKFLOW_TASK_STARTED                            
  48  WORKFLOW_TASK_COMPLETED                          
  49  WORKFLOW_EXECUTION_UPDATE_ACCEPTED               decide
  50  ACTIVITY_TASK_SCHEDULED                          issue_refund
  51  TIMER_CANCELED                                   
  52  TIMER_STARTED                                    timer 1800s
  53  ACTIVITY_TASK_STARTED                            
  54  ACTIVITY_TASK_COMPLETED                          
  55  WORKFLOW_TASK_SCHEDULED                          
  56  WORKFLOW_TASK_STARTED                            
  57  WORKFLOW_TASK_COMPLETED                          
  58  ACTIVITY_TASK_SCHEDULED                          deepagents.invoke_model
  59  ACTIVITY_TASK_STARTED                            
  60  ACTIVITY_TASK_COMPLETED                          
  61  WORKFLOW_TASK_SCHEDULED                          
  62  WORKFLOW_TASK_STARTED                            
  63  WORKFLOW_TASK_COMPLETED                          
  64  WORKFLOW_EXECUTION_UPDATE_COMPLETED              
  65  WORKFLOW_EXECUTION_SIGNALED                      close
  66  WORKFLOW_TASK_SCHEDULED                          
  67  WORKFLOW_TASK_STARTED                            
  68  WORKFLOW_TASK_COMPLETED                          
  69  TIMER_CANCELED                                   
  70  WORKFLOW_EXECUTION_COMPLETED                     

### Operator-cancelled order (order-ORD-1002)
   1  WORKFLOW_EXECUTION_STARTED                       OrderFulfillmentWorkflow
   2  WORKFLOW_TASK_SCHEDULED                          
   3  WORKFLOW_TASK_STARTED                            
   4  WORKFLOW_TASK_COMPLETED                          
   5  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   6  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
   7  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
   8  ACTIVITY_TASK_STARTED                            
   9  ACTIVITY_TASK_COMPLETED                          
  10  WORKFLOW_TASK_SCHEDULED                          
  11  WORKFLOW_TASK_STARTED                            
  12  WORKFLOW_TASK_COMPLETED                          
  13  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  14  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  15  ACTIVITY_TASK_STARTED                            
  16  ACTIVITY_TASK_COMPLETED                          
  17  WORKFLOW_TASK_SCHEDULED                          
  18  WORKFLOW_TASK_STARTED                            
  19  WORKFLOW_TASK_COMPLETED                          
  20  START_CHILD_WORKFLOW_EXECUTION_INITIATED         FraudAssessmentWorkflow
  21  CHILD_WORKFLOW_EXECUTION_STARTED                 FraudAssessmentWorkflow
  22  WORKFLOW_TASK_SCHEDULED                          
  23  WORKFLOW_TASK_STARTED                            
  24  WORKFLOW_TASK_COMPLETED                          
  25  CHILD_WORKFLOW_EXECUTION_COMPLETED               FraudAssessmentWorkflow
  26  WORKFLOW_TASK_SCHEDULED                          
  27  WORKFLOW_TASK_STARTED                            
  28  WORKFLOW_TASK_COMPLETED                          
  29  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  30  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  31  ACTIVITY_TASK_STARTED                            
  32  ACTIVITY_TASK_COMPLETED                          
  33  WORKFLOW_TASK_SCHEDULED                          
  34  WORKFLOW_TASK_STARTED                            
  35  WORKFLOW_TASK_COMPLETED                          
  36  ACTIVITY_TASK_SCHEDULED                          send_notification
  37  ACTIVITY_TASK_STARTED                            
  38  ACTIVITY_TASK_COMPLETED                          
  39  WORKFLOW_TASK_SCHEDULED                          
  40  WORKFLOW_TASK_STARTED                            
  41  WORKFLOW_TASK_COMPLETED                          
  42  TIMER_STARTED                                    timer 30s
  43  WORKFLOW_EXECUTION_CANCEL_REQUESTED              
  44  WORKFLOW_TASK_SCHEDULED                          
  45  WORKFLOW_TASK_STARTED                            
  46  WORKFLOW_TASK_COMPLETED                          
  47  TIMER_CANCELED                                   
  48  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES                
  49  ACTIVITY_TASK_SCHEDULED                          save_order_snapshot
  50  ACTIVITY_TASK_STARTED                            
  51  ACTIVITY_TASK_COMPLETED                          
  52  WORKFLOW_TASK_SCHEDULED                          
  53  WORKFLOW_TASK_STARTED                            
  54  WORKFLOW_TASK_COMPLETED                          
  55  WORKFLOW_EXECUTION_CANCELED                      
```
