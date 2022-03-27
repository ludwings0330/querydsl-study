package study.querydsl.entity;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;

@SpringBootTest
@Transactional
public class QuerydslMiddleTest {
    @PersistenceContext
    EntityManager em;

    JPAQueryFactory jpaQueryFactory;


    @BeforeEach
    public void before() {
        jpaQueryFactory = new JPAQueryFactory(em);

        final Team teamA = new Team("teamA");
        final Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    /**
     * 프로젝션 대상이 하나면 타입을 명확하게 지정할 수 있다.
     */
    @Test
    public void projectionOne() throws Exception {
        final List<String> result = jpaQueryFactory
                .select(member.username)
                .from(member)
                .fetch();

        assertThat(result.get(0)).isEqualTo("member1");
    }

    /**
     * 튜프 조회
     * 프로젝션 대상이 둘 이상일 때 Tuple 혹은 DTO 로 결과 조회
     */
    @Test
    public void projectionMultiple() throws Exception {
        final List<Tuple> result = jpaQueryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (var tuple :
                result) {
            final String username = tuple.get(member.username);
            final Integer age = tuple.get(member.age);

            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    /**
     * DTO 조회
     * 프로젝션 대상이 둘 이상일 때 Tuple 혹은 DTO 로 결과 조회
     * 순수 JPA 사용
     * 1. new 명령어 사용
     * 2. DTO 의 package 이름을 다 적어주어야 함
     * 3. 생성자 방식만 지원
     */
    @Test
    public void projectionMultipleDtoWithJPA() throws Exception {
        final List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) " +
                "from Member m", MemberDto.class)
                .getResultList();

        for (var member :
                result) {
            System.out.println("member.getUsername() = " + member.getUsername());
            System.out.println("member.getAge() = " + member.getAge());
        }
    }

    /**
     * DTO 조회
     * 결과를 DTO 반환할때 QueryDSL 사용시 3가지 방법 지원
     * 1. 프로퍼티 접근
     * 2. 필드 직접 접근
     * 3. 생성자 사용
     * (순수 JPA 의 경우 생성자 방식만 지원)
     */
    @Test
    public void beanPopulation() throws Exception {
//        1. 프로퍼티 접근 - setter
        final List<MemberDto> result = jpaQueryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

//        2. 필드 직접 접근
        final List<MemberDto> result2 = jpaQueryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

//        3. 생성자 사용
        final List<MemberDto> result3 = jpaQueryFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();
    }

    /**
     * 동적 쿼리 - BooleanBuilder 사용
     * 동적 쿼리를 해결하는 두가지 방식
     * 1. Boolean Builder
     * 2. Where 다중 파라미터 사용
     */
    @Test
    public void 동적쿼리_BooleanBuilder() throws Exception {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        final BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return jpaQueryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void 동적쿼리_WhereParam() throws Exception {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = serachMember2(usernameParam, ageParam);

        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> serachMember2(String usernameCond, Integer ageCond) {
//        return jpaQueryFactory
//                .selectFrom(member)
//                .where(usernameEq(usernameCond), ageEq(ageCond))
//                .fetch();
        return jpaQueryFactory
                .selectFrom(member)
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond) : null;
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    /**
     * 수정, 삭제 벌크 연산
     */
    @Test
    public void vulkFunc() throws Exception {
        final long count = jpaQueryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        final long count2 = jpaQueryFactory
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();

        final long count3 = jpaQueryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    /**
     * SQL function 호출하기
     */
    @Test
    public void callSqlFunction() throws Exception {
        // "member" 를 "M" 으로 replace
        final String result = jpaQueryFactory
                .select(Expressions.stringTemplate("function('replace', {0}, {1}, {2})", member.username, "member", "M"))
                .from(member)
                .fetchFirst();
        System.out.println("result = " + result);

        // 소문자로 변경해서 비교
        final String result2 = jpaQueryFactory
                .select(member.username)
                .from(member)
                .where(member.username.eq(Expressions.stringTemplate("function('lower', {0})", member.username)))
                .fetchFirst();

    }
}
