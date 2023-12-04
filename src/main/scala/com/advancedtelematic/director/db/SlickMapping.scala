package com.advancedtelematic.director.db

import com.advancedtelematic.director.data.AdminDataType.TargetUpdate
import com.advancedtelematic.director.data.Codecs._
import com.advancedtelematic.director.data.DataType.AdminRoleName
import com.advancedtelematic.libats.data.DataType.HashMethod
import com.advancedtelematic.libats.data.DataType.HashMethod.HashMethod
import com.advancedtelematic.libats.slick.db.SlickCirceMapper
import com.advancedtelematic.libtuf.data.ValidatedString.{ValidatedString, ValidatedStringValidation}
import slick.jdbc.MySQLProfile.api._

import scala.reflect.ClassTag

object SlickMapping {
  import com.advancedtelematic.libats.slick.codecs.SlickEnumMapper
  import com.advancedtelematic.libtuf.data.TufDataType.TargetFormat

  implicit val hashMethodColumn: slick.jdbc.MySQLProfile.BaseColumnType[com.advancedtelematic.libats.data.DataType.HashMethod.HashMethod] = MappedColumnType.base[HashMethod, String](_.toString, HashMethod.withName)
  implicit val targetFormatMapper: slick.jdbc.MySQLProfile.BaseColumnType[com.advancedtelematic.libtuf.data.TufDataType.TargetFormat.Value] = SlickEnumMapper.enumMapper(TargetFormat)

  implicit val targetUpdateMapper: slick.jdbc.MySQLProfile.BaseColumnType[com.advancedtelematic.director.data.AdminDataType.TargetUpdate] = SlickCirceMapper.circeMapper[TargetUpdate]

  private def validatedStringMapper[W <: ValidatedString : ClassTag](implicit validation: ValidatedStringValidation[W]) =
    MappedColumnType.base[W, String](
      _.value,
      validation.apply(_).valueOr(err => throw new IllegalArgumentException(err.toList.mkString))
    )

  implicit val adminRoleNameMapper: slick.jdbc.MySQLProfile.BaseColumnType[com.advancedtelematic.director.data.DataType.AdminRoleName] = validatedStringMapper[AdminRoleName]
}
