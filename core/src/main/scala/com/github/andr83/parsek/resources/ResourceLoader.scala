package com.github.andr83.parsek.resources

import com.github.andr83.parsek.PValue
import com.typesafe.config.Config

/**
 * @author andr83
 */
abstract class ResourceLoader {
  def read(config: Config): PValue
}
