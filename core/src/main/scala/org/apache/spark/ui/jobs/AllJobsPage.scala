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

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.HashMap
import scala.xml._

import org.apache.spark.scheduler._
import org.apache.spark.ui._
import org.apache.spark.ui.jobs.UIData.{JobUIData, StageUIData}

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val startTime = listener.startTime
      val endTime = listener.endTime
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse
      val failedJobs = listener.failedJobs.reverse

      val activeJobsTable =
        new JobsTable(request, activeJobs, "active", "activeJob", parent.basePath,
          parent.jobProgresslistener, killEnabled = parent.killEnabled)
      val completedJobsTable =
        new JobsTable(request, completedJobs, "completed", "completedJob", parent.basePath,
          parent.jobProgresslistener, killEnabled = false)
      val failedJobsTable =
        new JobsTable(request, failedJobs, "failed", "failedJob", parent.basePath,
          parent.jobProgresslistener, killEnabled = false)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val completedJobNumStr = if (completedJobs.size == listener.numCompletedJobs) {
        s"${completedJobs.size}"
      } else {
        s"${listener.numCompletedJobs}, only showing ${completedJobs.size}"
      }

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            <li>
              <strong>User:</strong>
              {parent.getSparkUser}
            </li>
            <li>
              <strong>Total Uptime:</strong>
              {
                if (endTime < 0 && parent.sc.isDefined) {
                  UIUtils.formatDuration(System.currentTimeMillis() - startTime)
                } else if (endTime > 0) {
                  UIUtils.formatDuration(endTime - startTime)
                }
              }
            </li>
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
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
                  {completedJobNumStr}
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  {listener.numFailedJobs}
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      val executorListener = parent.executorListener
      content ++= JobsUtils.makeTimeline(activeJobs ++ completedJobs ++ failedJobs,
          executorListener.executorEvents, startTime, parent.jobProgresslistener)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
          activeJobsTable.toNodeSeq
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobNumStr})</h4> ++
          completedJobsTable.toNodeSeq
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
          failedJobsTable.toNodeSeq
      }

      val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
        " Click on a job to see information about the stages of tasks inside it."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}

private[ui] class JobTableRowData(
    val jobData: JobUIData,
    val lastStageName: String,
    val lastStageDescription: String,
    val duration: Long,
    val formattedDuration: String,
    val submissionTime: Long,
    val formattedSubmissionTime: String,
    val jobDescription: NodeSeq,
    val detailUrl: String)

private[ui] class JobDataSource(
    jobs: Seq[JobUIData],
    stageIdToInfo: HashMap[Int, StageInfo],
    stageIdToData: HashMap[(Int, Int), StageUIData],
    basePath: String,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[JobTableRowData](pageSize) {

  // Convert JobUIData to JobTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = jobs.map(jobRow).sorted(ordering(sortColumn, desc))

  private var _slicedJobIds: Set[Int] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[JobTableRowData] = {
    val r = data.slice(from, to)
    _slicedJobIds = r.map(_.jobData.jobId).toSet
    r
  }

  private def getLastStageNameAndDescription(job: JobUIData): (String, String) = {
    val lastStageInfo = Option(job.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => stageIdToInfo.get(ids.max)}
    val lastStageData = lastStageInfo.flatMap { s =>
      stageIdToData.get((s.stageId, s.attemptId))
    }
    val name = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val description = lastStageData.flatMap(_.description).getOrElse("")
    (name, description)
  }

  private def jobRow(jobData: JobUIData): JobTableRowData = {
    val (lastStageName, lastStageDescription) = getLastStageNameAndDescription(jobData)
    val duration: Option[Long] = {
      jobData.submissionTime.map { start =>
        val end = jobData.completionTime.getOrElse(System.currentTimeMillis())
        end - start
      }
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
    val submissionTime = jobData.submissionTime
    val formattedSubmissionTime = submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
    val jobDescription = UIUtils.makeDescription(lastStageDescription, basePath, plainText = false)

    val detailUrl = "%s/jobs/job?id=%s".format(basePath, jobData.jobId)

    new JobTableRowData (
      jobData,
      lastStageName,
      lastStageDescription,
      duration.getOrElse(-1),
      formattedDuration,
      submissionTime.getOrElse(-1),
      formattedSubmissionTime,
      jobDescription,
      detailUrl
    )
  }

  /**
   * Return Ordering according to sortColumn and desc
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[JobTableRowData] = {
    val ordering: Ordering[JobTableRowData] = sortColumn match {
      case "Job Id" | "Job Id (Job Group)" => Ordering.by(_.jobData.jobId)
      case "Description" => Ordering.by(x => (x.lastStageDescription, x.lastStageName))
      case "Submitted" => Ordering.by(_.submissionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Stages: Succeeded/Total" | "Tasks (for all stages): Succeeded/Total" =>
        throw new IllegalArgumentException(s"Unsortable column: $sortColumn")
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }

}
private[ui] class JobPagedTable(
    data: Seq[JobUIData],
    tableHeaderId: String,
    jobTag: String,
    basePath: String,
    subPath: String,
    parameterOtherTable: Iterable[String],
    stageIdToInfo: HashMap[Int, StageInfo],
    stageIdToData: HashMap[(Int, Int), StageUIData],
    killEnabled: Boolean,
    currentTime: Long,
    jobIdTitle: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean
  ) extends PagedTable[JobTableRowData] {
  val parameterPath = basePath + s"/$subPath/?" + parameterOtherTable.mkString("&")

  override def tableId: String = jobTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = jobTag + ".pageSize"

  override def prevPageSizeFormField: String = jobTag + ".prevPageSize"

  override def pageNumberFormField: String = jobTag + ".page"

  override val dataSource = new JobDataSource(
    data,
    stageIdToInfo,
    stageIdToData,
    basePath,
    currentTime,
    pageSize,
    sortColumn,
    desc)

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$jobTag.sort=$encodedSortColumn" +
      s"&$jobTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$parameterPath&$jobTag.sort=$encodedSortColumn&$jobTag.desc=$desc#$tableHeaderId"
  }

  override def headers: Seq[Node] = {
    // Information for each header: title, cssClass, and sortable
    val jobHeadersAndCssClasses: Seq[(String, String, Boolean)] =
      Seq(
        (jobIdTitle, "", true),
        ("Description", "", true), ("Submitted", "", true), ("Duration", "", true),
        ("Stages: Succeeded/Total", "", false),
        ("Tasks (for all stages): Succeeded/Total", "", false)
      )

    if (!jobHeadersAndCssClasses.filter(_._3).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      jobHeadersAndCssClasses.map { case (header, cssClass, sortable) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$jobTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&$jobTag.desc=${!desc}" +
              s"&$jobTag.pageSize=$pageSize" +
              s"#$tableHeaderId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th class={cssClass}>
            <a href={headerLink}>
              {header}<span>
              &nbsp;{Unparsed(arrow)}
            </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$jobTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
                s"&$jobTag.pageSize=$pageSize" +
                s"#$tableHeaderId")

            <th class={cssClass}>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          } else {
            <th class={cssClass}>
              {header}
            </th>
          }
        }
      }
    }
    <thead>{headerRow}</thead>
  }

  override def row(jobTableRow: JobTableRowData): Seq[Node] = {
    val job = jobTableRow.jobData

    val killLink = if (killEnabled) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill job ${job.jobId} ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      // SPARK-6846 this should be POST-only but YARN AM won't proxy POST
      /*
      val killLinkUri = s"$basePathUri/jobs/job/kill/"
      <form action={killLinkUri} method="POST" style="display:inline">
        <input type="hidden" name="id" value={job.jobId.toString}/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
       */
      val killLinkUri = s"$basePath/jobs/job/kill/?id=${job.jobId}"
      <a href={killLinkUri} onclick={confirm} class="kill-link">(kill)</a>
    } else {
      Seq.empty
    }

    <tr id={"job-" + job.jobId}>
      <td>
        {job.jobId} { job.jobGroup.map { id =>
          <a href={"%s/jobs/jobgroup?id=%s".format(basePath, id)} class="name-link">
            {id}
          </a>
        }.getOrElse({job.jobGroup.map(id => s"($id)").getOrElse("")})}
      </td>
      <td>
        {jobTableRow.jobDescription} {killLink}
        <a href={jobTableRow.detailUrl} class="name-link">{jobTableRow.lastStageName}</a>
      </td>
      <td>
        {jobTableRow.formattedSubmissionTime}
      </td>
      <td>{jobTableRow.formattedDuration}</td>
      <td class="stage-progress-cell">
        {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
        {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
        {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
      </td>
      <td class="progress-cell">
        {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
        failed = job.numFailedTasks, skipped = job.numSkippedTasks,
        reasonToNumKilled = job.reasonToNumKilled, total = job.numTasks - job.numSkippedTasks)}
      </td>
    </tr>
  }
}
