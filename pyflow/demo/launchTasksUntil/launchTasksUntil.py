#!/usr/bin/env python
#
# pyFlow - a lightweight parallel task engine
#
# Copyright (c) 2012-2017 Illumina, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#


import os.path
import sys

# add module path by hand
#
scriptDir=os.path.abspath(os.path.dirname(__file__))
sys.path.append(scriptDir+"/../../src")

from pyflow import WorkflowRunner


#
# very simple task scripts called by the demo:
#
testJobDir=os.path.join(scriptDir)

workerJob=os.path.join(testJobDir,"testWorker.py")



def lockMethod(f):
    """
    method decorator acquires/releases object's lock
    """

    def wrapped(self, *args, **kw):
        import threading
        if not hasattr(self,"lock") :
            self.lock = threading.RLock()

        self.lock.acquire()
        try:
            return f(self, *args, **kw)
        finally:
            self.lock.release()
    return wrapped



class SyncronizedAccumulator(object) :

    def __init__(self) :
        self._values = []

    @lockMethod
    def addOrderedValue(self, index, value):
        if index+1 > len(self._values) :
            self._values.append(None)
        self._values[index] = value

    @lockMethod
    def numberOfCompleteTasks(self):
        count = 0
        for v in self._values :
            if v is None : continue
            count += 1
        return count

    @lockMethod
    def numberOfContinuousCompleteTasks(self):
        count = 0
        for v in self._values :
            if v is None : break
            count += 1
        return count

    @lockMethod
    def totalValue(self):
        sum = 0
        for v in self._values :
            if v is None : continue
            sum += v
        return sum

    @lockMethod
    def totalContinuousValue(self):
        sum = 0
        for v in self._values :
            if v is None : break
            sum += v
        return sum



class SumWorkflow(WorkflowRunner) :

    def __init__(self, taskIndex, inputFile, totalWorkCompleted) :
        self.taskIndex=taskIndex
        self.inputFile=inputFile
        self.totalWorkCompleted=totalWorkCompleted

    def workflow(self) :
        import os
        infp = open(self.inputFile, "rb")
        value = int(infp.read().strip())
        infp.close()
        os.remove(self.inputFile)
        self.totalWorkCompleted.addOrderedValue(taskIndex,value)


class LauncherWorkflow(WorkflowRunner) :

    def __init__(self):
        self.totalWorkTarget = 100

    def workflow(self):

        allTasks = set()
        completedTasks = set()
        totalWorkCompleted = SyncronizedAccumulator()

        def launchNextTask() :
            taskIndex = len(allTasks)
            workerTaskLabel = "workerTask%05i" % (taskIndex)
            workerTaskFile = "outputFile%05i" % (taskIndex)
            workerTaskCmd=[sys.executable, workerJob, workerTaskFile]
            self.addTask(workerTaskLabel, workerTaskCmd)

            allTasks.insert(workerTaskLabel)

            sumTaskLabel="sumTask%05i" % (taskIndex)
            self.addWorkflowTask(sumTaskLabel, SumWorkflow(taskIndex, workerTaskFile, totalWorkCompleted), dependencies=workerTaskLabel)

        def updateCompletedTasks() :
            for task in allTasks :
                if task in completedTasks : continue
                if not self.isTaskComplete(task) : continue
                completedTasks.insert(task)

        maxTaskCount = self.getNCores()
        assert(maxTaskCount > 0)

        import time
        while True :
            completedWork = totalWorkCompleted.getCount()
            self.flowLog("TotalWorkCompleted: %i" % (completedWork))
            if completedWork >= self.totalWorkTarget : break

            updateCompletedTasks()
            runningTaskCount = len(allTasks)-len(completedTasks)
            self.flowLog("Completed/Running tasks: %i %i" % (len(completedTasks), runningTaskCount))
            assert(runningTaskCount >= 0)

            numberOfTasksToLaunch = max(maxTaskCount-runningTaskCount,0)

            for _ in range(numberOfTasksToLaunch) : launchNextTask()

            time.sleep(5)



class LaunchUntilWorkflow(WorkflowRunner) :


    def workflow(self) :

        self.addWorkflowTask("launcherWorkflow",LauncherWorkflow())
        # A simple command task with no dependencies, labeled 'task1'.
        #
        cmd="%s 1" % (yelljob)
        self.addTask("task1",cmd)

        # Another task which runs the same command, this time the
        # command is provided as an argument list. An argument list
        # can be useful when a command has many arguments or
        # complicated quoting issues:
        #
        cmd=[yelljob,"1"]
        self.addTask("task2",cmd)

        # This task will always run on the local machine, no matter
        # what the run mode is. The force local option is useful for
        # non-cpu intensive jobs which are taking care of minor
        # workflow overhead (moving/touching files, etc)
        #
        self.addTask("task3a",sleepjob+" 10",isForceLocal=True)

        # This job is requesting 2 threads:
        #
        self.addTask("task3b",runjob+" 10",nCores=2)

        # This job is requesting 2 threads and 3 gigs of ram:
        #
        self.addTask("task3c",runjob+" 10",nCores=2,memMb=3*1024)


        # addTask and addWorkflowTask always return their task labels
        # as a simple convenience. taskName is set to "task4" now.
        #
        taskName=self.addTask("task4",sleepjob+" 1")

        # an example task dependency:
        #
        # pyflow stores dependencies in set() objects, but you can
        # provide a list,tuple,set or single string as the argument to
        # dependencies:
        #
        # all the task5* tasks below specify "task4" as their
        # dependency:
        #
        self.addTask("task5a",yelljob+" 2",dependencies=taskName)
        self.addTask("task5b",yelljob+" 2",dependencies="task4")
        self.addTask("task5c",yelljob+" 2",dependencies=["task4"])
        self.addTask("task5d",yelljob+" 2",dependencies=[taskName])

        # this time we launch a number of sleep tasks based on the
        # workflow parameters:
        #
        # we store all tasks in sleepTasks -- which we use to make
        # other tasks wait for this entire set of jobs to complete:
        #
        sleepTasks=set()
        for i in range(self.params["numSleepTasks"]) :
            taskName="sleep_task%i" % (i)
            sleepTasks.add(taskName)
            self.addTask(taskName,sleepjob+" 1",dependencies="task5a")

            ## note the three lines above could have been written in a
            ## more compact single-line format:
            ##
            #sleepTasks.add(self.addTask("sleep_task%i" % (i),sleepjob+" 1",dependencies="task5a"))

        # this job cannot start until all tasks in the above loop complete:
        self.addTask("task6",runjob+" 2",nCores=3,dependencies=sleepTasks)

        # This task is supposed to fail, uncomment to see error reporting:
        #
        #self.addTask("task7",sleepjob)

        # Note that no command is provided to this task. It will not
        # be distributed locally or to sge, but does provide a
        # convenient label for a set of tasks that other processes
        # depend on. There is no special "checkpoint-task" type in
        # pyflow -- but any task can function like one per this
        # example:
        #
        self.addTask("checkpoint_task",dependencies=["task1","task6","task5a"])

        # The final task depends on the above checkpoint:
        #
        self.addTask("task8",yelljob+" 2",dependencies="checkpoint_task")



wflow = LaunchUntilWorkflow()

# Run the worklow:
#
retval=wflow.run(mode="local",nCores=3)

sys.exit(retval)

