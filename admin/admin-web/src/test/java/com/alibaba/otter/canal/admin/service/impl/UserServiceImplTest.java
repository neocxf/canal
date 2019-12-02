package com.alibaba.otter.canal.admin.service.impl;

import com.alibaba.otter.canal.admin.CanalAdminApplicationTest;
import com.alibaba.otter.canal.admin.service.UserService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class UserServiceImplTest extends CanalAdminApplicationTest {
    @Autowired
    UserService userService;

    @Test
    public void testFind() {
        userService.find4Login("admin", "admin1");
    }

}
