package com.aymene.mapper;

import com.aymene.model.dto.PersonDto;
import com.aymene.protobuf.PersonOuterClass.Person;

public class PersonMapper {

    public static Person toProtobuf(PersonDto dto) {
        Person.Builder builder = Person.newBuilder()
                .setName(dto.name())
                .setId(dto.id());

        if (dto.email() != null && !dto.email().isBlank()) {
            builder.setEmail(dto.email());
        }

        return builder.build();
    }
}
