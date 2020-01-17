package com.yushkevich.foo.message.payload;

import lombok.*;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@ToString(callSuper = true)
public class FooBar extends Foo {
    private Long uniqueId;

    @Builder(builderMethodName = "fooBarBuilder")
    public FooBar(Long id, Long originalId, String value, Long uniqueId) {
        super(id, originalId, value);
        this.uniqueId = uniqueId;
    }
}
