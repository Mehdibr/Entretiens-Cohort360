from django.urls import path, include
from rest_framework.routers import SimpleRouter

from .views import PatientViewSet, MedicationViewSet

router = SimpleRouter(trailing_slash=False)
router.register(r"Patient", PatientViewSet, basename="patient")
router.register(r"Medication", MedicationViewSet, basename="medication")

urlpatterns = [
    path("", include(router.urls)),
]
