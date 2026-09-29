package org.example.notifyservice.consumer;

import org.example.notifyservice.service.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class EnrollmentCreatedConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrollmentCreatedConsumer.class);

    private final EmailService emailService;

    public EnrollmentCreatedConsumer(EmailService emailService) {
        this.emailService = emailService;
    }

    @KafkaListener(topics = "${notification.kafka.enrollment-created-topic:enrollment-created}")
    public void consume(String email) {
        if (!StringUtils.hasText(email)) {
            LOGGER.warn("Ignored blank enrollment-created messag");
            return;
        }

        emailService.sendEnrollmentCreatedEmail(email);
    }
}
