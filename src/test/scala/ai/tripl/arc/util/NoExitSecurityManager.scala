package ai.tripl.arc.util

import java.security.Permission

// this security manager intercepts sys.exit calls and returns an ExitOKException or ExitErrorException but does not exit the program
class NoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}

  override def checkPermission(perm: Permission, context: Object): Unit = {}

  override def checkExit(status: Int): Unit = {
    super.checkExit(status)
    if (status == 0) throw ExitOKException() else throw ExitErrorException()
  }
}

sealed case class ExitErrorException() extends SecurityException("System.exit() intercepted") {}
sealed case class ExitOKException() extends SecurityException("System.exit() intercepted") {}
