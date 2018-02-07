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

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, Unparsed, Utility}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.apache.spark.ui.{ToolTips, UIUtils}
import org.apache.spark.ui.jobs.UIData.JobUIData

/** Utility functions for generating Jobs related XML pages with spark content. */
private object JobsUtils extends Logging {
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

  def getLastStageNameAndDescription(
    job: JobUIData, progressListener: JobProgressListener): (String, String) = {
    val lastStageInfo = Option(job.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => progressListener.stageIdToInfo.get(ids.max)}
    val lastStageData = lastStageInfo.flatMap { s =>
      progressListener.stageIdToData.get((s.stageId, s.attemptId))
    }
    val name = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val description = lastStageData.flatMap(_.description).getOrElse("")
    (name, description)
  }

  def makeJobEvent(
    jobUIDatas: Seq[JobUIData], progressListener: JobProgressListener): Seq[String] = {
    jobUIDatas.filter { jobUIData =>
      jobUIData.status != JobExecutionStatus.UNKNOWN && jobUIData.submissionTime.isDefined
    }.map { jobUIData =>
      val jobId = jobUIData.jobId
      val status = jobUIData.status
      val (jobName, jobDescription) = getLastStageNameAndDescription(jobUIData, progressListener)
      val displayJobDescription =
        if (jobDescription.isEmpty) {
          jobName
        } else {
          UIUtils.makeDescription(jobDescription, "", plainText = true).text
        }
      val submissionTime = jobUIData.submissionTime.get
      val completionTimeOpt = jobUIData.completionTime
      val completionTime = completionTimeOpt.getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedDesc = Utility.escape(displayJobDescription)
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

  def makeExecutorEvent(executorUIDatas: Seq[SparkListenerEvent]):
    Seq[String] = {
    val events = ListBuffer[String]()
    executorUIDatas.foreach {
      case a: SparkListenerExecutorAdded =>
        val addedEvent =
          s"""
             |{
             |  'className': 'executor added',
             |  'group': 'executors',
             |  'start': new Date(${a.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${a.executorId}<br>' +
             |    'Added at ${UIUtils.formatDate(new Date(a.time))}"' +
             |    'data-html="true">Executor ${a.executorId} added</div>'
             |}
           """.stripMargin
        events += addedEvent
      case e: SparkListenerExecutorRemoved =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${e.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${e.executorId}<br>' +
             |    'Removed at ${UIUtils.formatDate(new Date(e.time))}' +
             |    '${
            if (e.reason != null) {
              s"""<br>Reason: ${e.reason.replace("\n", " ")}"""
            } else {
              ""
            }
          }"' +
             |    'data-html="true">Executor ${e.executorId} removed</div>'
             |}
           """.stripMargin
        events += removedEvent

    }
    events.toSeq
  }

  def makeTimeline(
    jobs: Seq[JobUIData],
    executors: Seq[SparkListenerEvent],
    startTime: Long,
    progressListener: JobProgressListener): Seq[Node] = {

    val jobEventJsonAsStrSeq = makeJobEvent(jobs, progressListener)
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
}
