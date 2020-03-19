package ai.tripl.arc.util
import scala.language.implicitConversions

// Only needed for Scala 2.11.x can remove when supporting only 2.12.x and greater
object EitherUtils {

    class MappableEither[L, R1](e: Either[L, R1]) {

        def rightFlatMap[R2](mapper: R1 => Either[L, R2]): Either[L, R2] = {
            e.fold( Left(_) , mapper(_) )
        }

        def |>[R2](mapper: R1 => Either[L, R2]): Either[L, R2] = rightFlatMap(mapper)
    }

    implicit def eitherToMappableEither[L, R](e: Either[L, R]) = new MappableEither(e)

}