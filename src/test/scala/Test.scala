import com.own.hydropower.common_params.Comm_Params

object Test extends App {
  Comm_Params.initConfig()

  println(Comm_Params.hbasePath)
}
