# Remoulade Scheduling Example

This example demonstrates how to use the scheduler of remoulade.

### Jobs
- Count the words of https://github.com every second
- Count the words of https://gitlab.com every ten seconds

### Files in the example
- task_scheduler.py: describe the task you want to schedule
- scheduler.py: start the worker and the scheduler
- utils.py: utilitary functions to start the worker and  scheduler

### Instructions

###### first terminal
```sh #
$ rabbitmq-server
```

###### second terminal
```sh # second terminal
$ redis-server
```

###### third terminal
```sh # third terminal
$ python3 scheduler.py
```
