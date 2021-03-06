package study.querydsl.entity;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

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

    @Test
    public void startQuerydsl2() throws Exception {
        final Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() throws Exception {
        final Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    public void searchAndParam() throws Exception {
        final List<Member> result1 = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"),
                        member.age.eq(10))
                .fetch();

        assertThat(result1.size()).isEqualTo(1);
    }


    /**
     * ?????? ?????? ??????
     * 1. ?????? ?????? ????????????(desc)
     * 2. ?????? ?????? ????????????(asc)
     * ??? 2?????? ?????? ????????? ????????? ???????????? ??????(nulls last)
     */
    @Test
    public void sort() throws Exception {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        final List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        final Member member5 = result.get(0);
        final Member member6 = result.get(1);
        final Member member7 = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(member7.getUsername()).isNull();
    }

    @Test
    public void paging1() throws Exception {
        final List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0?????? ???(zero index)
                .limit(2) // ?????? 2??? ??????
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }


    /**
     * JPQL
     * select
     *  COUNT(m), // ?????????
     *  SUM(m.age), // ?????? ???
     *  AVG(m.age), // ?????? ??????
     *  MAX(m.age), // ?????? ??????
     *  MIN(m.age) // ?????? ??????
     * from Member m
     */
    @Test
    public void aggregation() throws Exception {
        final List<Tuple> result = jpaQueryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();

        final Tuple tuple = result.get(0);

        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * ?????? ????????? ??? ?????? ?????? ????????? ?????????
     */
    @Test
    public void group() throws Exception {
        final List<Tuple> result = jpaQueryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        final Tuple teamA = result.get(0);
        final Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * ??? A??? ????????? ?????? ??????
     * join(?????? ??????, ???????????? ????????? Q??????)
     */
    @Test
    public void join() throws Exception {
        final List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

//    /**
//     * GROUP BY
//     * HAVING ???
//     */
//    @Test
//    public void having() throws Exception {
//        final List<Member> result = jpaQueryFactory
//                .selectFrom(member)
//                .join(member.team, team)
//                .where(team.name.eq("teamA"))
//                .having(member.username.eq("member1"))
//                .fetch();
//
//        assertThat(result.get(0).getUsername()).isEqualTo("member1");
//    }

    /**
     * ?????? ??????(??????????????? ?????? ????????? ??????)
     * ????????? ????????? ??? ????????? ?????? ?????? ??????
     */

    @Test
    public void theta_join() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        final List<Member> result = jpaQueryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * ????????? ?????? ???????????????, ??? ????????? teamA??? ?????? ??????, ????????? ?????? ??????
     * JPQL: SELECT m, t FROM Member m LEFT JOIN m.team t on t.name = 'teamA'
     * SQL : SELECT m.*, t.* FROM Member m LEFT JOIN Team t ON m.TEAM_ID=t.id and t.name='teamA'
     */

    @Test
    public void join_on_filtering() throws Exception {
        final List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple :
             result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 2. ???????????? ?????? ????????? ?????? ??????
     * ???) ????????? ????????? ?????? ????????? ?????? ?????? ?????? ??????
     * JPQL : SELECT m, t FROM Member m LEFT JOIN Team t on m.username = t.name
     * SQL : SELECT m.*, t.* FROM Member LEFT JOIN Team t ON m.username = t.name
     */
    @Test
    public void join_on_no_relation() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        final List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple :
                result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() throws Exception {
        em.flush();
        em.clear();

        final Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        final boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("?????? ?????? ?????????").isFalse();
    }

    @Test
    public void fetchJoinUse() throws Exception {
        em.flush();
        em.clear();

        final Member findMember = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        final boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("?????? ?????? ?????????").isTrue();
    }

    /**
     * ?????? ??????
     * ????????? ?????? ?????? ????????? ??????
     */
    @Test
    public void subQuery() throws Exception {
        final QMember memberSub = new QMember("memberSub");
        final List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * ????????? ?????? ????????? ????????? ??????
     */
    @Test
    public void subQueryGoe() throws Exception {
        final QMember memberSub = new QMember("memberSub");

        final List<Member> result = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * IN ??? ?????? ?????? ??????
     */

    @Test
    public void subQueryIn() throws Exception {
        final QMember memberSub = new QMember("memberSub");

        final List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }

    /**
     * SELECT ?????? SUb QUERY
     */
    @Test
    public void subQuerySelect() throws Exception {
        final QMember memberSub = new QMember("memberSub");

        final List<Tuple> result = jpaQueryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple :
                result) {
            System.out.println("tuple.get(member.username) = " + tuple.get(member.username));
            System.out.println("tuple.get(JPAExpressions.select(memberSub.age.avg().from(memberSub))) = " +
                    tuple.get(JPAExpressions.select(memberSub.age.avg()).from(memberSub)));
        }
    }

    /**
     * CASE ???
     * SELECT, WHERE< ORDER BY ?????? ?????? ??????
     */
    @Test
    public void caseQuery() throws Exception {
        final List<String> result = jpaQueryFactory
                .select(member.age
                        .when(10).then("??????")
                        .when(20).then("?????????")
                        .otherwise("??????"))
                .from(member)
                .fetch();

        assertThat(result.get(0)).isEqualTo("??????");
    }

    /**
     * CASE ????????? ??????
     * CaseBuilder, otherwise
     */
    @Test
    public void complexCaseQuery() throws Exception {
        final List<String> result = jpaQueryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20???")
                        .when(member.age.between(21, 30)).then("21~30???")
                        .otherwise("??????"))
                .from(member)
                .fetch();

        assertThat(result.get(0)).isEqualTo("0~20???");
        assertThat(result.get(1)).isEqualTo("0~20???");
        assertThat(result.get(2)).isEqualTo("21~30???");
        assertThat(result.get(3)).isEqualTo("??????");
    }

    /**
     * ORDER BY + CASE ?????? ??????
     * 1. 0 ~ 30 ?????? ?????? ????????? ?????? ?????? ??????
     * 2. 0 ~ 20 ??? ?????? ??????
     * 3. 21 ~ 30??? ?????? ??????
     */

    @Test
    public void orderByAndCase() throws Exception {
        final NumberExpression<Integer> rankPath = new CaseBuilder()
                .when(member.age.between(0, 20)).then(2)
                .when(member.age.between(21, 30)).then(1)
                .otherwise(3);

        final List<Tuple> result = jpaQueryFactory
                .select(member.username, member.age, rankPath)
                .from(member)
                .orderBy(rankPath.desc())
                .fetch();


        for (var tuple :
                result) {
            final String username = tuple.get(member.username);
            final Integer age = tuple.get(member.age);
            final Integer rank = tuple.get(rankPath);

            System.out.println("username = " + username + " age = " + age + " rank = " + rank);
        }
    }

    /**
     * ??????, ?????? ?????????
     * Expressions.constant(xxx) ??????
     */
    @Test
    public void constAdd() throws Exception {
        final Tuple result = jpaQueryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetchFirst();

        System.out.println("result.get(member.username) = " + result.get(member.username));

        final String result2 = jpaQueryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(result2).isEqualTo("member1_10");
    }
}
