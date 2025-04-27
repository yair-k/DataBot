package routes

import (
	"databot-ai/internal/apis/middlewares"
	"databot-ai/internal/di"
	"log"

	"github.com/gin-gonic/gin"
)

func SetupAuthRoutes(router *gin.Engine) {
	authHandler, err := di.GetAuthHandler()
	if err != nil {
		log.Fatalf("Failed to get auth handler: %v", err)
	}

	// Auth routes
	auth := router.Group("/api/auth")
	{
		auth.POST("/signup", authHandler.Signup)
		auth.POST("/login", authHandler.Login)
		auth.POST("/generate-signup-secret", authHandler.GenerateUserSignupSecret)
	}

	protected := router.Group("/api/auth")
	protected.Use(middlewares.AuthMiddleware())
	{
		protected.GET("/", authHandler.GetUser)
		protected.POST("/logout", authHandler.Logout)
		protected.GET("/refresh-token", authHandler.RefreshToken)
	}
}
