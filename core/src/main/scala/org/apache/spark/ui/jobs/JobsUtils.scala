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

import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.xml.{Node, Unparsed, Utility}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.util.Utils

private object JobsUtils extends Logging{
  import ApiHelper._

  val JOBS_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="succeeded-job-legend"
            x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Succeeded</text>
      <rect class="failed-job-legend"
            x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="running-job-legend"
            x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Running</text>
    </svg></div>.toString.filter(_ != '\n')

  val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
            x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
            x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  private def makeJobEvent(store: AppStatusStore, jobs: Seq[v1.JobData]): Seq[String] = {
    jobs.filter { job =>
      job.status != JobExecutionStatus.UNKNOWN && job.submissionTime.isDefined
    }.map { job =>
      val jobId = job.jobId
      val status = job.status
      val (_, lastStageDescription) = lastStageNameAndDescription(store, job)
      val jobDescription = UIUtils.makeDescription(lastStageDescription, "", plainText = true).text

      val submissionTime = job.submissionTime.get.getTime()
      val completionTime = job.completionTime.map(_.getTime()).getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedDesc = Utility.escape(jobDescription)
      val jsEscapedDesc = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-html="true" data-placement="top" data-toggle="tooltip"' +
           |     'data-title="${jsEscapedDesc} (Job ${jobId})<br>' +
           |     'Status: ${status}<br>' +
           |     'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
          if (status != JobExecutionStatus.RUNNING) {
            s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
          } else {
            ""
          }
        }">' +
           |    '${jsEscapedDesc} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]):
  Seq[String] = {
    val events = ListBuffer[String]()
    executors.foreach { e =>
      val addedEvent =
        s"""
           |{
           |  'className': 'executor added',
           |  'group': 'executors',
           |  'start': new Date(${e.addTime.getTime()}),
           |  'content': '<div class="executor-event-content"' +
           |    'data-toggle="tooltip" data-placement="bottom"' +
           |    'data-title="Executor ${e.id}<br>' +
           |    'Added at ${UIUtils.formatDate(e.addTime)}"' +
           |    'data-html="true">Executor ${e.id} added</div>'
           |}
         """.stripMargin
      events += addedEvent

      e.removeTime.foreach { removeTime =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${removeTime.getTime()}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${e.id}<br>' +
             |    'Removed at ${UIUtils.formatDate(removeTime)}' +
             |    '${
            e.removeReason.map { reason =>
              s"""<br>Reason: ${reason.replace("\n", " ")}"""
            }.getOrElse("")
          }"' +
             |    'data-html="true">Executor ${e.id} removed</div>'
             |}
           """.stripMargin
        events += removedEvent
      }
    }
    events.toSeq
  }

  def makeTimeline(
      store: AppStatusStore,
      jobs: Seq[v1.JobData],
      executors: Seq[v1.ExecutorSummary],
      startTime: Long): Seq[Node] = {

    val jobEventJsonAsStrSeq = makeJobEvent(store, jobs)
    val executorEventJsonAsStrSeq = makeExecutorEvent(executors)

    val groupJsonArrayAsStr =
      s"""
         |[
         |  {
         |    'id': 'executors',
         |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
         |  },
         |  {
         |    'id': 'jobs',
         |    'content': '<div>Jobs</div>${JOBS_LEGEND}',
         |  }
         |]
        """.stripMargin

    val eventArrayAsStr =
      (jobEventJsonAsStrSeq ++ executorEventJsonAsStrSeq).mkString("[", ",", "]")

    <span class="expand-application-timeline">
      <span class="expand-application-timeline-arrow arrow-closed"></span>
      <a data-toggle="tooltip" title={ToolTips.JOB_TIMELINE} data-placement="right">
        Event Timeline
      </a>
    </span> ++
      <div id="application-timeline" class="collapsed">
        <div class="control-panel">
          <div id="application-timeline-zoom-lock">
            <input type="checkbox"></input>
            <span>Enable zooming</span>
          </div>
        </div>
      </div> ++
      <script type="text/javascript">
        {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}," +
        s"${eventArrayAsStr}, ${startTime}, ${UIUtils.getTimeZoneOffset()});")}
      </script>
  }

  def jobsTable(
      store: AppStatusStore,
      basePath: String,
      request: HttpServletRequest,
      tableHeaderId: String,
      jobTag: String,
      jobs: Seq[v1.JobData],
      killEnabled: Boolean): Seq[Node] = {
    // stripXSS is called to remove suspicious characters used in XSS attacks
    val allParameters = request.getParameterMap.asScala.toMap.map { case (k, v) =>
      UIUtils.stripXSS(k) -> v.map(UIUtils.stripXSS).toSeq
    }
    val parameterOtherTable = allParameters.filterNot(_._1.startsWith(jobTag))
      .map(para => para._1 + "=" + para._2(0))

    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)
    val jobIdTitle = if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"

    // stripXSS is called first to remove suspicious characters used in XSS attacks
    val parameterJobPage = UIUtils.stripXSS(request.getParameter(jobTag + ".page"))
    val parameterJobSortColumn = UIUtils.stripXSS(request.getParameter(jobTag + ".sort"))
    val parameterJobSortDesc = UIUtils.stripXSS(request.getParameter(jobTag + ".desc"))
    val parameterJobPageSize = UIUtils.stripXSS(request.getParameter(jobTag + ".pageSize"))
    val parameterJobPrevPageSize = UIUtils.stripXSS(request.getParameter(jobTag + ".prevPageSize"))

    val jobPage = Option(parameterJobPage).map(_.toInt).getOrElse(1)
    val jobSortColumn = Option(parameterJobSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(jobIdTitle)
    val jobSortDesc = Option(parameterJobSortDesc).map(_.toBoolean).getOrElse(
      // New jobs should be shown above old jobs by default.
      jobSortColumn == jobIdTitle
    )
    val jobPageSize = Option(parameterJobPageSize).map(_.toInt).getOrElse(100)
    val jobPrevPageSize = Option(parameterJobPrevPageSize).map(_.toInt).getOrElse(jobPageSize)

    val page: Int = {
      // If the user has changed to a larger page size, then go to page 1 in order to avoid
      // IndexOutOfBoundsException.
      if (jobPageSize <= jobPrevPageSize) {
        jobPage
      } else {
        1
      }
    }
    val currentTime = System.currentTimeMillis()

    try {
      new JobPagedTable(
        store,
        jobs,
        tableHeaderId,
        jobTag,
        UIUtils.prependBaseUri(basePath),
        "jobs", // subPath
        parameterOtherTable,
        killEnabled,
        currentTime,
        jobIdTitle,
        pageSize = jobPageSize,
        sortColumn = jobSortColumn,
        desc = jobSortDesc
      ).table(page)
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering job table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}
