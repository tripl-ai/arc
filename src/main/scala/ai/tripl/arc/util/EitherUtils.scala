package ai.tripl.arc.util
import scala.language.implicitConversions

object EitherUtils {

    class MappableEither[L, R1](e: Either[L, R1]) {
        def |>[R2](mapper: R1 => Either[L, R2]): Either[L, R2] = e.flatMap(mapper(_)) 
    }

    implicit def eitherToMappableEither[L, R](e: Either[L, R]) = new MappableEither(e)

}