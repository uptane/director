package com.advancedtelematic.director.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.director.data.Generators
import com.advancedtelematic.director.db.RepoNamespaceRepositorySupport
import com.advancedtelematic.director.util.{DirectorSpec, RepositorySpec, RouteResourceSpec}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import cats.syntax.show._
import io.circe.syntax._
import com.advancedtelematic.director.data.Codecs._
import com.advancedtelematic.libats.data.ErrorRepresentation
import com.advancedtelematic.libtuf.data.ClientDataType.RemoteSessionsRole
import com.advancedtelematic.libtuf.data.TufDataType.SignedPayload
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import com.advancedtelematic.libtuf.data.ClientCodecs._
import com.advancedtelematic.libtuf.data.TufCodecs._
import org.scalatest.EitherValues._

class RemoteSessionsRoutesSpec extends DirectorSpec
with RouteResourceSpec
with RepoNamespaceRepositorySupport
with AdminResources
with RepositorySpec
with Generators
with DeviceResources {

  testWithRepo("can set a remote session for a device") { implicit ns =>
    val deviceId = DeviceId.generate()

    val session = Map("mysession" -> "mysession attrs")

    val body = RemoteSessionRequest(session.asJson, 0)

    Post(apiUri(s"admin/remote-sessions"), body).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val signedRole = responseAs[SignedPayload[RemoteSessionsRole]].signed
      signedRole.remote_sessions.as[Map[String, String]].value shouldBe session
    }

    Get(apiUri(s"device/${deviceId.show}/remote-sessions.json"), body).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val signedRole = responseAs[SignedPayload[RemoteSessionsRole]].signed
      signedRole.remote_sessions.as[Map[String, String]].value shouldBe session
    }

    Get(apiUri(s"admin/remote-sessions.json"), body).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK

      val signedRole = responseAs[SignedPayload[RemoteSessionsRole]].signed
      signedRole.remote_sessions.as[Map[String, String]].value shouldBe session
    }
  }

  testWithRepo("updating with wrong previousVersion returns conflict") { implicit ns =>
    val session = Map("mysession" -> "mysession attrs")

    val body = RemoteSessionRequest(session.asJson, 0)

    Post(apiUri(s"admin/remote-sessions"), body).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.OK
      val signedRole = responseAs[SignedPayload[RemoteSessionsRole]].signed
      signedRole.remote_sessions.as[Map[String, String]].value shouldBe session
    }

    val body2 = RemoteSessionRequest(session.asJson, 3)

    Post(apiUri(s"admin/remote-sessions"), body2).namespaced ~> routes ~> check {
      status shouldBe StatusCodes.Conflict

      val error = responseAs[ErrorRepresentation]
      error.code.code shouldBe "invalid_version_bump"
    }
  }
}
