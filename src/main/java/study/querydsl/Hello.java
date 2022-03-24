package study.querydsl;

import javax.annotation.processing.Generated;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class Hello {
    @Id @GeneratedValue
    private Long id;
    private String name;
}
