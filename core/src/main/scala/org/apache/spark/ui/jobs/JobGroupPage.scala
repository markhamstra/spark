/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.jobs.UIData.JobUIData

/** Page showing list of jobs under a job group id */
private[ui] class JobGroupPage(parent: JobsTab) extends WebUIPage("jobgroup") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing parameter id")

      val jobGroupId = parameterId
      val groupToJobsTable = listener.jobGroupToJobIds.get(jobGroupId)
      if (groupToJobsTable.isEmpty) {
        val content =
          <div id="no-info">
            <p>No information to display for jobGroup
              {jobGroupId}
            </p>
          </div>
        return UIUtils.headerSparkPage(
          s"Details for JobGroup $jobGroupId", content, parent)
      }

      val jobsInGroup = listener.jobIdToData

      val activeJobsInGroup = mutable.Buffer[JobUIData]()
      val completedJobsInGroup = mutable.Buffer[JobUIData]()
      val failedJobsInGroup = mutable.Buffer[JobUIData]()
      var totalDuration = 0L
      groupToJobsTable.get.foreach { jobId =>
        val job = jobsInGroup.get(jobId)
        val duration: Option[Long] = {
          job.get.submissionTime.map { start =>
            val end = job.get.completionTime.getOrElse(System.currentTimeMillis())
            end - start
          }
        }
        totalDuration += duration.getOrElse(0L)
        job.get.status match {
          case JobExecutionStatus.RUNNING => activeJobsInGroup ++= job
          case JobExecutionStatus.SUCCEEDED => completedJobsInGroup ++= job
          case JobExecutionStatus.FAILED => failedJobsInGroup ++= job
          case JobExecutionStatus.UNKNOWN => // not handling unknown status
        }
      }

      val activeJobsTable =
        new JobsTable(request,
          activeJobsInGroup.sortBy(_.submissionTime.getOrElse((-1L))).reverse,
          "active", "activeJob", parent.basePath, listener, killEnabled = parent.killEnabled)
      val completedJobsTable =
        new JobsTable(request,
          completedJobsInGroup.sortBy(_.completionTime.getOrElse(-1L)).reverse,
          "completed", "completeJob", parent.basePath, listener, killEnabled = false)
      val failedJobsTable =
        new JobsTable(request,
          failedJobsInGroup.sortBy(_.completionTime.getOrElse(-1L)).reverse,
          "failed", "failedJob", parent.basePath, listener, killEnabled = false)

      val shouldShowActiveJobs = activeJobsInGroup.nonEmpty
      val shouldShowCompletedJobs = completedJobsInGroup.nonEmpty
      val shouldShowFailedJobs = failedJobsInGroup.nonEmpty

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total Duration:</strong>
              {
              if (totalDuration == 0) {
                "Unknown"
              } else {
                UIUtils.formatDuration(totalDuration)
              }
              }
            </li>
            <li>
              <strong>Total jobs submitted:</strong>
              {activeJobsInGroup.size + completedJobsInGroup.size + failedJobsInGroup.size}
            </li>
            {if (shouldShowActiveJobs) {
            <li>
              <a href="#active"><strong>Active Jobs:</strong></a>
              {activeJobsInGroup.size}
            </li>
          }}{if (shouldShowCompletedJobs) {
            <li id="completed-summary">
              <a href="#completed"><strong>Completed Jobs:</strong></a>
              {completedJobsInGroup.size}
            </li>
          }}{if (shouldShowFailedJobs) {
            <li>
              <a href="#failed"><strong>Failed Jobs:</strong></a>
              {failedJobsInGroup.size}
            </li>
          }}
          </ul>
        </div>

      var content = summary
      val executorListener = parent.executorListener
      content ++= JobsUtils.makeTimeline(
        activeJobsInGroup ++ completedJobsInGroup ++ failedJobsInGroup,
        executorListener.executorEvents, listener.startTime, listener)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobsInGroup.size})</h4> ++
          activeJobsTable.toNodeSeq
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobsInGroup.size})</h4> ++
          completedJobsTable.toNodeSeq
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id="failed">Failed Jobs ({failedJobsInGroup.size})</h4> ++
          failedJobsTable.toNodeSeq
      }

      val helpText =
        s"""A job is triggered by an action, like count() or saveAsTextFile().
            | Click on a job to see information about the stages of tasks inside it.""".stripMargin

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}
