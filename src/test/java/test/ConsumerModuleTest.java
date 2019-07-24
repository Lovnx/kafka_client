package test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-17 14:23
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring-service.xml"})
public class ConsumerModuleTest {

    @Test
    public void test() throws InterruptedException {
        Thread.sleep(5000);
    }

}
