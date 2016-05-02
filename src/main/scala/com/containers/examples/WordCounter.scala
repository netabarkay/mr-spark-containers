package com.containers.examples

import com.containers.infrastrcture.Containers.HasContainerFunctions

/**
  * User: netab
  * Date: 5/2/16
  */
object WordCounter {


  def count[C[_]](input : C[String])
                 (implicit hcf : HasContainerFunctions[C]) : C[(String, Long)] = {

    val words = hcf.flatMap(input)(_.split("\\s+"))

    hcf.aggregateByKey(hcf.map(input)((_, 1L)))(0L)(_ + _, _ + _)

  }

  // With infix notation
  def countVer2[C[_]](input : C[String])
                     (implicit hcf : HasContainerFunctions[C]) : C[(String, Long)] = {

    import com.containers.infrastrcture.Containers.HasContainerFunctionsOperators
    import com.containers.infrastrcture.Containers.HasPairContainerFunctionsOperators

    input
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      .aggregateByKey(0L)(_ + _, _ + _)
  }

}
