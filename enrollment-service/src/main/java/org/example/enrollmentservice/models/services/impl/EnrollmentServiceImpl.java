package org.example.enrollmentservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.enrollmentservice.models.constants.EnrollmentStatus;
import org.example.enrollmentservice.exceptions.DuplicateCourseException;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentDetailRequest;
import org.example.enrollmentservice.models.dto.requests.CreateEnrollmentRequest;
import org.example.enrollmentservice.models.dto.responses.EnrollmentDetailResponse;
import org.example.enrollmentservice.models.dto.responses.EnrollmentResponse;
import org.example.enrollmentservice.models.dto.responses.CourseResponse;
import org.example.enrollmentservice.models.entities.Enrollment;
import org.example.enrollmentservice.models.entities.EnrollmentDetail;
import org.example.enrollmentservice.models.repositories.EnrollmentDetailRepository;
import org.example.enrollmentservice.models.repositories.EnrollmentRepository;
import org.example.enrollmentservice.models.services.EnrollmentService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class EnrollmentServiceImpl implements EnrollmentService {

        private static final String ENROLLMENT_CREATED_TOPIC = "enrollment-created";

        private final EnrollmentRepository enrollmentRepository;
        private final EnrollmentDetailRepository enrollmentDetailRepository;
        private final CourseGatewayService courseGatewayService;
        private final KafkaTemplate<String, String> kafkaTemplate;

        @Override
        @Transactional
        public EnrollmentResponse createEnrollment(CreateEnrollmentRequest request) {
                List<CourseResponse> courses = getRequestedCourses(request.items());

                Enrollment enrollment = Enrollment.builder()
                        .studentName(request.studentName())
                        .studentEmail(request.studentEmail())
                        .status(EnrollmentStatus.PENDING)
                        .totalFee(calculateTotalFee(courses))
                        .build();

                Enrollment savedEnrollment = enrollmentRepository.save(enrollment);
                List<EnrollmentDetail> savedDetails = enrollmentDetailRepository.saveAll(
                        createEnrollmentDetails(savedEnrollment, courses)
                );

                kafkaTemplate.send(ENROLLMENT_CREATED_TOPIC, request.studentEmail());

                return toResponse(savedEnrollment, savedDetails, courses);
        }

        private List<CourseResponse> getRequestedCourses(List<CreateEnrollmentDetailRequest> items) {
                Set<Long> courseIds = new HashSet<>();
                List<CourseResponse> courses = new ArrayList<>();

                for (CreateEnrollmentDetailRequest item : items) {
                        if (!courseIds.add(item.courseId())) {
                                throw new DuplicateCourseException();
                        }
                        courses.add(courseGatewayService.getCourseById(item.courseId()));
                }

                return courses;
        }

        private Double calculateTotalFee(List<CourseResponse> courses) {
                return courses.stream()
                        .mapToDouble(CourseResponse::courseFee)
                        .sum();
        }

        private List<EnrollmentDetail> createEnrollmentDetails(Enrollment enrollment, List<CourseResponse> courses) {
                List<EnrollmentDetail> details = new ArrayList<>();

                for (CourseResponse course : courses) {
                        details.add(EnrollmentDetail.builder()
                                .enrollment(enrollment)
                                .courseId(course.courseId())
                                .courseFee(course.courseFee())
                                .build());
                }

                return details;
        }

        private EnrollmentResponse toResponse(
                Enrollment enrollment,
                List<EnrollmentDetail> details,
                List<CourseResponse> courses
        ) {
                List<EnrollmentDetailResponse> detailResponses = new ArrayList<>();

                for (int i = 0; i < details.size(); i++) {
                        EnrollmentDetail detail = details.get(i);
                        CourseResponse course = courses.get(i);

                        detailResponses.add(new EnrollmentDetailResponse(
                                detail.getId(),
                                detail.getCourseId(),
                                course.courseName(),
                                detail.getCourseFee(),
                                detail.getCourseFee()
                        ));
                }

                return new EnrollmentResponse(
                        enrollment.getId(),
                        enrollment.getStudentName(),
                        enrollment.getStudentEmail(),
                        enrollment.getTotalFee(),
                        enrollment.getStatus(),
                        detailResponses
                );
        }

}
