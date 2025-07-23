package com.aymene.producer.converter;

import com.aymene.protobuf.PersonOuterClass;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class JsonToProtobufConverter {

    private JsonToProtobufConverter() {
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static PersonOuterClass.Person fromJsonFile(File file) throws IOException {
        // Create a simple intermediate POJO
        SimplePerson simplePerson = objectMapper.readValue(file, SimplePerson.class);

        return PersonOuterClass.Person.newBuilder()
                .setName(simplePerson.name)
                .setId(simplePerson.id)
                .setEmail(simplePerson.email != null ? simplePerson.email : "")
                .build();
    }



    private static class SimplePerson {
        public String name;
        public int id;
        public String email;
    }
}
