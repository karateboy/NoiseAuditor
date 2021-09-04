package controllers

import models.{Ability, GroupOp, User, UserOp}
import play.api.libs.json._
import play.api.mvc._
case class Credential(user: String, password: String)
import javax.inject._
import models.Group
case class UserData(user:User, group:Group)
/**
 * @author user
 */
class Login @Inject()
(userOp: UserOp, groupOp:GroupOp)
  extends Controller {
  implicit val credentialReads = Json.reads[Credential]

  def authenticate = Action(BodyParsers.parse.json){
    implicit request =>
      val credentail = request.body.validate[Credential]
      credentail.fold(
          error=>{
            BadRequest(Json.obj("ok"->false, "msg"->JsError.toJson(error)))
          },
          crd=>{
            val userOpt = userOp.getUserByEmail(crd.user)
            if(userOpt.isEmpty || userOpt.get.password != crd.password) {
              Results.Unauthorized(Json.obj("ok"->false, "msg"->"密碼或帳戶錯誤"))
            } else {
              implicit val writes = Json.writes[User]
              implicit val w3 = Json.writes[Ability]
              implicit val w1 = Json.writes[Group]
              implicit val w2 = Json.writes[UserData]

              val user = userOpt.get
              val userGroup = {
                user.group.getOrElse({
                  if(user.isAdmin)
                    Group.PLATFORM_ADMIN
                  else
                    Group.PLATFORM_USER
                })
              }
              val userInfo = UserInfo(user._id, user.name, userGroup, user.isAdmin)
              val group = groupOp.getGroupByID(userGroup).get
              Ok(Json.obj("ok"->true, "userData"->UserData(user, group))).withSession(Security.setUserinfo(request, userInfo))
            }              
          })
  }

  def isLogin = Security.Authenticated {
    Ok(Json.obj("ok"->true))
  }

  def logout = Action{
    Ok("").withNewSession
  }
}