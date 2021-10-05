package com.skraba.byexample;

import com.skraba.avro.enchiridion.simple.DateLogicalTypeOptionalRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

@Path("/hello")
public class GreetingResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String hello() throws IOException {
    DateLogicalTypeOptionalRecord original =
        DateLogicalTypeOptionalRecord.newBuilder()
            .setName("Hello world!")
            .setDatetimeMs(null)
            .setDatetimeUs(null)
            .setLocalDatetimeMs(null)
            .setLocalDatetimeUs(null)
            .setDate(LocalDate.of(1922, 01, 17))
            .setTimeMs(null)
            .setTimeUs(null)
            .build();

    final byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<DateLogicalTypeOptionalRecord> w =
          new SpecificDatumWriter<>(DateLogicalTypeOptionalRecord.class);
      w.write(original, encoder);
      encoder.flush();
      serialized = baos.toByteArray();
    }

    final DateLogicalTypeOptionalRecord roundTrip;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<DateLogicalTypeOptionalRecord> r =
          new SpecificDatumReader<>(DateLogicalTypeOptionalRecord.class);
      roundTrip = r.read(null, decoder);
    }

    return roundTrip.getName().toString() + " " + roundTrip.getDate().toString();
  }
}
