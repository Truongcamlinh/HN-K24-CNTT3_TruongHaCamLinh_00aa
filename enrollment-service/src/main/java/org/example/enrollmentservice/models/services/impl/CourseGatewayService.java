package org.example.enrollmentservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.example.enrollmentservice.clients.CourseClient;
import org.example.enrollmentservice.exceptions.CourseNotFoundException;
import org.example.enrollmentservice.exceptions.CourseServiceException;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CourseGatewayService {

    private final CourseClient courseClient;

    @CircuitBreaker(name = "courseService", fallbackMethod = "fallbackGetCourseById")
    public CourseResponse getCourseById(Long courseId) {
        try {
            return courseClient.getCourseById(courseId);
        } catch (FeignException.NotFound exception) {
            throw new CourseNotFoundException(courseId);
        } catch (FeignException exception) {
            throw new CourseServiceException("Course service is unavailable", exception);
        }
    }

    private CourseResponse fallbackGetCourseById(Long courseId, Throwable throwable) {
        if (throwable instanceof CourseNotFoundException) {
            throw (CourseNotFoundException) throwable;
        }

        if (throwable instanceof FeignException.NotFound) {
            throw new CourseNotFoundException(courseId);
        }

        throw new CourseServiceException("Course service is unavailable", throwable);
    }

}
