package org.apache.orc.impl;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.io.EOFException;
import java.io.IOException;

/** A BitFieldReader which reads 1 bit at a time. */
public class SingleBitFieldReader {
  private final RunLengthByteReader input;
  private int current;
  private int bitsLeft;

  public SingleBitFieldReader(InStream input) {
    this.input = new RunLengthByteReader(input);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  public int next() throws IOException {
    if (bitsLeft == 0) {
      readByte();
    }
    bitsLeft -= 1;
    return (current >>> bitsLeft) & 1;
  }


  public void skip(long items) throws IOException {
    long totalBits = items;
    if (bitsLeft >= totalBits) {
      bitsLeft -= totalBits;
    } else {
      totalBits -= bitsLeft;
      input.skip(totalBits / 8);
      current = input.next();
      bitsLeft = (int) (8 - (totalBits % 8));
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current + " bits left: " + bitsLeft +
            " bit size: 1 from " + input;
  }
}