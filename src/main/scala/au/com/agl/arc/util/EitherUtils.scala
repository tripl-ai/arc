package au.com.agl.arc.util

object EitherUtils {

    class MappableEither[L, R1](e: Either[L, R1]) {

        def rightFlatMap[R2](mapper: R1 => Either[L, R2]): Either[L, R2] = {
            e.fold( Left(_) , mapper(_) )
        }
    }

    implicit def eitherToMappableEither[L, R](e: Either[L, R]) = new MappableEither(e)

}