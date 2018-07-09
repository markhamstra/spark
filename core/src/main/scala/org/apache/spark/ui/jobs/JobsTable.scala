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

import scala.collection.JavaConverters._

import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.jobs.UIData.JobUIData
import org.apache.spark.util.Utils

private[ui] class JobsTable(
    request: HttpServletRequest,
    jobs: Seq[JobUIData],
    tableHeaderId: String,
    jobTag: String,
    basePath: String,
    progressListener: JobProgressListener,
    killEnabled: Boolean) {
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
    if (jobSortColumn == jobIdTitle) true else false
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

  val toNodeSeq = try {
    new JobPagedTable(
      jobs,
      tableHeaderId,
      jobTag,
      UIUtils.prependBaseUri(basePath),
      "jobs", // subPath
      parameterOtherTable,
      progressListener.stageIdToInfo,
      progressListener.stageIdToData,
      killEnabled,
      currentTime,
      jobIdTitle,
      pageSize = jobPageSize,
      sortColumn = jobSortColumn,
      desc = jobSortDesc
    ).table(page)
  } catch {
    case e@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
      <div class="alert alert-error">
        <p>Error while rendering job table:</p>
        <pre>
          {Utils.exceptionString(e)}
        </pre>
      </div>
  }
}
