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

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, NodeSeq}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._

/** Page showing list of all ongoing and recently finished jobs belonging to a job group id */
private[ui] class JobGroupPage(parent: JobsTab, store: AppStatusStore)
    extends WebUIPage("jobgroup") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val jobGroupId = parameterId

    val activeJobs = new ListBuffer[v1.JobData]()
    val completedJobs = new ListBuffer[v1.JobData]()
    val failedJobs = new ListBuffer[v1.JobData]()

    var totalJobExecutionTime = 0L
    store.jobsInJobGroupList(jobGroupId, null).foreach { job =>
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.map(_.getTime).getOrElse(System.currentTimeMillis())
          end - start.getTime()
        }
      }

      totalJobExecutionTime += duration.getOrElse(0L)

      job.status match {
        case JobExecutionStatus.SUCCEEDED =>
          completedJobs += job
        case JobExecutionStatus.FAILED =>
          failedJobs += job
        case _ =>
          activeJobs += job
      }
    }

    val activeJobsTable =
      JobsUtils.jobsTable(store, parent.basePath, request, "active",
        "activeJob", activeJobs, killEnabled = parent.killEnabled)
    val completedJobsTable =
      JobsUtils.jobsTable(store, parent.basePath, request, "completed",
        "completedJob", completedJobs, killEnabled = false)
    val failedJobsTable =
      JobsUtils.jobsTable(store, parent.basePath, request, "failed",
        "failedJob", failedJobs, killEnabled = false)

    val shouldShowActiveJobs = activeJobs.nonEmpty
    val shouldShowCompletedJobs = completedJobs.nonEmpty
    val shouldShowFailedJobs = failedJobs.nonEmpty

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Total Duration:</strong>
            {
            if (totalJobExecutionTime == 0) {
              "Unknown"
            } else {
              UIUtils.formatDuration(totalJobExecutionTime)
            }
            }
          </li>
          <li>
            <strong>Total jobs submitted:</strong>
            {activeJobs.size + completedJobs.size + failedJobs.size}
          </li>
          {
          if (shouldShowActiveJobs) {
            <li>
              <a href="#active"><strong>Active Jobs:</strong></a>
              {activeJobs.size}
            </li>
          }
          }
          {
          if (shouldShowCompletedJobs) {
            <li id="completed-summary">
              <a href="#completed"><strong>Completed Jobs:</strong></a>
              {completedJobs.size}
            </li>
          }
          }
          {
          if (shouldShowFailedJobs) {
            <li>
              <a href="#failed"><strong>Failed Jobs:</strong></a>
              {failedJobs.size}
            </li>
          }
          }
        </ul>
      </div>

    var content = summary
    val appStartTime = store.applicationInfo().attempts.head.startTime.getTime()

    content ++= JobsUtils.makeTimeline(store, activeJobs ++ completedJobs ++ failedJobs,
      store.executorList(false), appStartTime)

    if (shouldShowActiveJobs) {
      content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
        activeJobsTable
    }
    if (shouldShowCompletedJobs) {
      content ++= <h4 id="completed">Completed Jobs ({completedJobs.size})</h4> ++
        completedJobsTable
    }
    if (shouldShowFailedJobs) {
      content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
        failedJobsTable
    }

    val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
      " Click on a job to see information about the stages of tasks inside it."

    UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
  }

}
