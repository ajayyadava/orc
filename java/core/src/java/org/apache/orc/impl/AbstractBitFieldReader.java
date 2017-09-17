package org.apache.orc.impl;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.io.IOException;

public abstract class AbstractBitFieldReader {

  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (previous.noNulls || !previous.isNull[i]) {
        previous.vector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((previous.vector[0] != previous.vector[i]) ||
          (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      bitsLeft = 8 - consumed;
    } else {
      bitsLeft = 0;
    }
  }

  abstract long next();

}
