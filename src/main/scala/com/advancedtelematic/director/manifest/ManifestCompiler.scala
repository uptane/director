package com.advancedtelematic.director.manifest

import cats.syntax.option._
import com.advancedtelematic.director.data.Codecs.ecuManifestCustomCodec
import com.advancedtelematic.director.data.DbDataType.{Assignment, DeviceKnownState, EcuTarget, EcuTargetId, ProcessedAssignment}
import com.advancedtelematic.director.data.DeviceRequest._
import com.advancedtelematic.director.data.UptaneDataType.Image
import com.advancedtelematic.director.http.Errors
import com.advancedtelematic.libats.data.DataType.{Checksum, CorrelationId, HashMethod, Namespace, ResultCode, ResultDescription}
import com.advancedtelematic.libats.data.EcuIdentifier
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, EcuInstallationReport, InstallationResult}
import com.advancedtelematic.libats.messaging_datatype.MessageCodecs._
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeviceUpdateCompleted, DeviceUpdateEvent}
import io.circe.syntax._
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.util.{Failure, Success, Try}


object ManifestCompiler {
  private val _log = LoggerFactory.getLogger(this.getClass)

  case class ManifestCompileResult(knownState: DeviceKnownState, messages: List[DeviceUpdateEvent])

  private def assignmentExists(assignments: Set[Assignment],
                               ecuTargets: Map[EcuTargetId, EcuTarget],
                               ecuIdentifier: EcuIdentifier, ecuManifest: EcuManifest): Option[Assignment] = {
    assignments.find { assignment =>
      val installedPath = ecuManifest.installed_image.filepath
      val installedChecksum = ecuManifest.installed_image.fileinfo.hashes.sha256

      _log.debug(s"looking for ${assignment.ecuTargetId} in $ecuTargets")

      val assignmentTarget = ecuTargets(assignment.ecuTargetId)

      assignment.ecuId == ecuIdentifier &&
        assignmentTarget.filename == installedPath &&
        assignmentTarget.checksum.hash == installedChecksum
    }
  }

  def apply(ns: Namespace, manifest: DeviceManifest): DeviceKnownState => Try[ManifestCompileResult] = (beforeState: DeviceKnownState) => {
    validateManifest(manifest, beforeState).map { _ =>
      val (nextStatus, msgs) = compileManifest(ns, manifest).apply(beforeState)
      ManifestCompileResult(nextStatus, msgs)
    }
  }

  private def validateManifest(manifest: DeviceManifest, deviceKnownStatus: DeviceKnownState): Try[DeviceManifest] = {
    if(manifest.primary_ecu_serial != deviceKnownStatus.primaryEcu)
      Failure(Errors.Manifest.EcuNotPrimary)
    else
      Success(manifest)
  }

  private def compileManifest(ns: Namespace, manifest: DeviceManifest): DeviceKnownState => (DeviceKnownState, List[DeviceUpdateEvent]) = (knownStatus: DeviceKnownState) => {
    _log.debug(s"current device state: $knownStatus")

    val assignmentsProcessedInManifest = manifest.ecu_version_manifests.flatMap { case (ecuId, signedManifest) =>
      assignmentExists(knownStatus.currentAssignments, knownStatus.ecuTargets, ecuId, signedManifest.signed)
    }

    val newEcuTargets = manifest.ecu_version_manifests.values.map { ecuManifest =>
      val installedImage = ecuManifest.signed.installed_image
      val existingEcuTarget = findEcuTargetByImage(knownStatus.ecuTargets, installedImage)

      if (existingEcuTarget.isEmpty) {
        _log.debug(s"$installedImage not found in ${knownStatus.ecuTargets}")
        EcuTarget(ns, EcuTargetId.generate, installedImage.filepath, installedImage.fileinfo.length,
          Checksum(HashMethod.SHA256, installedImage.fileinfo.hashes.sha256), installedImage.fileinfo.hashes.sha256,
          uri = None, userDefinedCustom = None)
      } else
        existingEcuTarget.get

    }.map(e => e.id -> e).toMap

    val statusInManifest = manifest.ecu_version_manifests.mapValues { ecuManifest =>
      val newTargetO = findEcuTargetByImage(newEcuTargets, ecuManifest.signed.installed_image)
      newTargetO.map(_.id)
    }.filter(_._2.isDefined)

    val status = DeviceKnownState(
      knownStatus.deviceId,
      knownStatus.primaryEcu,
      knownStatus.ecuStatus ++ statusInManifest,
      knownStatus.ecuTargets ++ newEcuTargets,
      currentAssignments = Set.empty,
      processedAssignments = Set.empty,
      generatedMetadataOutdated = knownStatus.generatedMetadataOutdated)

    val installationReport = manifest.installation_report.map(_.report)

    installationReport match {
      case Left(MissingInstallationReport) =>
        val desc = s"Missing installation report, assignments processed = $assignmentsProcessedInManifest"
        _log.info(s"${knownStatus.deviceId} " + desc)

        val newStatus = status.copy(
          currentAssignments = knownStatus.currentAssignments -- assignmentsProcessedInManifest,
          processedAssignments = knownStatus.processedAssignments ++ assignmentsProcessedInManifest.map(_.toProcessedAssignment(successful = true, canceled = false, desc.some))
        )

        val msgs = resultMsgFromEcuManifests(ns, knownStatus, manifest)

        newStatus -> msgs

      case Left(InvalidInstallationReport(reason, payload)) =>
        val desc = s"${knownStatus.deviceId} Device sent an invalid installation report ($reason). Cancelling current assignments"
        _log.info(desc)
        val cancelled = assignmentsProcessedInManifest.isEmpty
        val cancelledAssignments = knownStatus.currentAssignments.map(_.toProcessedAssignment(successful = false, canceled = cancelled, desc.some))

        val newStatus = status.copy(
          currentAssignments = Set.empty,
          processedAssignments = knownStatus.processedAssignments ++ cancelledAssignments
        )

        val msgs = buildMessagesFromCancelledAssignments(ns, knownStatus.deviceId, desc, cancelledAssignments,
          ResultCodes.DeviceSentInvalidInstallationReport, payload.map(_.noSpaces))

        newStatus -> msgs

      case Right(report) if !report.result.success =>
        val _report = manifest.installation_report.map(_.report.result).map(_.asJson.noSpaces)
        val desc = s"Device reported installation error: ${_report}"
        _log.info(s"${knownStatus.deviceId} Received error installation report: $desc")

        val cancelled = assignmentsProcessedInManifest.isEmpty
        val cancelledAssignments = knownStatus.currentAssignments.map(_.toProcessedAssignment(successful = false, canceled = cancelled, desc.some))

        val newStatus = status.copy(
          currentAssignments = Set.empty,
          processedAssignments = knownStatus.processedAssignments ++ cancelledAssignments,
          generatedMetadataOutdated = true
        )

        val processedCorrelationIds = assignmentsProcessedInManifest.map(_.correlationId).toList

        val msgs = if(assignmentsProcessedInManifest.isEmpty)
          buildMessagesFromCancelledAssignments(ns, knownStatus.deviceId, desc, cancelledAssignments, ResultCodes.DirectorCancelledAssignment)
        else
          resultMsgFromInstallationReport(ns, knownStatus.deviceId, manifest, processedCorrelationIds, report)

        newStatus -> msgs

      case Right(_) if assignmentsProcessedInManifest.isEmpty =>
        val desc = "Device sent a successful installation report, but no assignments were processed in the manifest"
        _log.info(s"${knownStatus.deviceId} " + desc)

        val cancelledAssignments = knownStatus.currentAssignments.map(_.toProcessedAssignment(successful = false, canceled = true, desc.some))

        val newStatus = status.copy(
          currentAssignments = Set.empty,
          processedAssignments = knownStatus.processedAssignments ++ cancelledAssignments,
          generatedMetadataOutdated = true
        )

        val msgs = buildMessagesFromCancelledAssignments(ns, knownStatus.deviceId, desc, cancelledAssignments, ResultCodes.DirectorCancelledAssignment)

        newStatus -> msgs

      case Right(report) if assignmentsProcessedInManifest.nonEmpty =>
        val desc = "Device sent a successful installation report"
        _log.info(s"${knownStatus.deviceId} Device sent a successful installation report, processing assignments $assignmentsProcessedInManifest")

        val newStatus = status.copy(
          currentAssignments = knownStatus.currentAssignments -- assignmentsProcessedInManifest,
          processedAssignments = knownStatus.processedAssignments ++ assignmentsProcessedInManifest.map(_.toProcessedAssignment(successful = true, canceled = false, desc.some))
        )

        val correlationIds = assignmentsProcessedInManifest.map(_.correlationId).toList
        val msg = resultMsgFromInstallationReport(ns, knownStatus.deviceId, manifest, correlationIds, report)

        newStatus -> msg
    }
  }

  private def buildMessagesFromCancelledAssignments(ns: Namespace, deviceId: DeviceId, desc: String, cancelledAssignments: Iterable[ProcessedAssignment],
                                                    resultCode: ResultCode, rawReport: Option[String] = None) = {
    if (cancelledAssignments.nonEmpty) {
      val now = Instant.now
      val rdesc = ResultDescription(desc)
      val ir = InstallationResult(success = false, code = resultCode, description = rdesc)

      cancelledAssignments.toList.map { assignment =>
        DeviceUpdateCompleted(ns, now, assignment.correlationId, deviceId, ir, Map.empty, rawReport)
      }
    } else
      List.empty
  }

  private def findEcuTargetByImage(ecuTargets: Map[EcuTargetId, EcuTarget], image: Image): Option[EcuTarget] = {
    ecuTargets.values.find { ecuTarget =>
      val imagePath = image.filepath
      val imageChecksum = image.fileinfo.hashes.sha256

      ecuTarget.filename == imagePath && ecuTarget.checksum.hash == imageChecksum
    }
  }

  private def resultMsgFromInstallationReport(namespace: Namespace,
                                              deviceId: DeviceId,
                                              deviceManifest: DeviceManifest,
                                              processedCorrelationIds: List[CorrelationId],
                                              report: InstallationReport,
                                             ): List[DeviceUpdateCompleted] = {

    val reportedImages = deviceManifest.ecu_version_manifests.mapValues { ecuManifest =>
      ecuManifest.signed.installed_image.filepath
    }

    val ecuResults = report.items
      .filter(item => reportedImages.contains(item.ecu))
      .map { item =>
        item.ecu -> EcuInstallationReport(item.result, Seq(reportedImages(item.ecu).toString))
      }.toMap

    val now = Instant.now

    processedCorrelationIds.map { cid =>
      DeviceUpdateCompleted(namespace, now, cid, deviceId, report.result, ecuResults, report.raw_report)
    }
  }

  // Some legacy devices do not send an installation report, so we need to extract the result from ecu reports
  private def resultMsgFromEcuManifests(namespace: Namespace, beforeState: DeviceKnownState, manifest: DeviceManifest): List[DeviceUpdateCompleted] = {
    val manifests = manifest.ecu_version_manifests.flatMap { case (ecuId, signedManifest) =>
      assignmentExists(beforeState.currentAssignments, beforeState.ecuTargets, ecuId, signedManifest.signed).map { a =>
        a.correlationId -> signedManifest.signed
      }
    }

    manifests.map { case (correlationId, ecuReport) =>
      val ecuReports = ecuReport.custom.flatMap(_.as[EcuManifestCustom].toOption).map { custom =>
        val operationResult = custom.operation_result
        val installationResult = InstallationResult(operationResult.isSuccess, ResultCode(operationResult.result_code.toString), ResultDescription(operationResult.result_text))
        Map(ecuReport.ecu_serial -> EcuInstallationReport(installationResult, Seq(ecuReport.installed_image.filepath.toString)))
      }.getOrElse(Map.empty)

      val installationResult = if (ecuReports.exists(!_._2.result.success))
        InstallationResult(success = false, ResultCode("19"), ResultDescription("One or more targeted ECUs failed to update"))
      else
        InstallationResult(success = true, ResultCode("0"), ResultDescription("All targeted ECUs were successfully updated"))

      DeviceUpdateCompleted(namespace, Instant.now(), correlationId, beforeState.deviceId, installationResult, ecuReports)
    }.toList
  }

}
