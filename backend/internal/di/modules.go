package di

import (
	"databot-ai/config"
	"databot-ai/internal/apis/handlers"
	"databot-ai/internal/constants"
	"databot-ai/internal/repositories"
	"databot-ai/internal/services"
	"databot-ai/internal/utils"
	"databot-ai/pkg/dbmanager"
	"databot-ai/pkg/llm"
	"databot-ai/pkg/mongodb"
	"databot-ai/pkg/redis"
	"log"
	"time"

	"go.uber.org/dig"
)

var DiContainer *dig.Container

func Initialize() {
	DiContainer = dig.New()

	// Initialize MongoDB
	dbConfig := mongodb.MongoDbConfigModel{
		ConnectionUrl: config.Env.MongoURI,
		DatabaseName:  config.Env.MongoDatabaseName,
	}
	mongodbClient := mongodb.InitializeDatabaseConnection(dbConfig)

	// Initialize Redis
	redisClient, err := redis.RedisClient(config.Env.RedisHost, config.Env.RedisPort, config.Env.RedisUsername, config.Env.RedisPassword)
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}

	// Initialize services and repositories
	redisRepo := redis.NewRedisRepositories(redisClient)
	jwtService := utils.NewJWTService(
		config.Env.JWTSecret,
		time.Millisecond*time.Duration(config.Env.JWTExpirationMilliseconds),
		time.Millisecond*time.Duration(config.Env.JWTRefreshExpirationMilliseconds),
	)

	// Initialize token repository
	tokenRepo := repositories.NewTokenRepository(redisRepo)

	chatRepo := repositories.NewChatRepository(mongodbClient)
	llmRepo := repositories.NewLLMMessageRepository(mongodbClient)

	// Provide all dependencies to the container
	if err := DiContainer.Provide(func() *mongodb.MongoDBClient { return mongodbClient }); err != nil {
		log.Fatalf("Failed to provide MongoDB client: %v", err)
	}

	if err := DiContainer.Provide(func() redis.IRedisRepositories { return redisRepo }); err != nil {
		log.Fatalf("Failed to provide Redis repositories: %v", err)
	}

	if err := DiContainer.Provide(func() utils.JWTService { return jwtService }); err != nil {
		log.Fatalf("Failed to provide JWT service: %v", err)
	}

	if err := DiContainer.Provide(func() repositories.ChatRepository { return chatRepo }); err != nil {
		log.Fatalf("Failed to provide chat repository: %v", err)
	}

	if err := DiContainer.Provide(func() repositories.LLMMessageRepository { return llmRepo }); err != nil {
		log.Fatalf("Failed to provide LLM message repository: %v", err)
	}

	// Provide DB Manager
	if err := DiContainer.Provide(func(redisRepo redis.IRedisRepositories) (*dbmanager.Manager, error) {
		encryptionKey := config.Env.SchemaEncryptionKey
		manager, err := dbmanager.NewManager(redisRepo, encryptionKey)
		if err != nil {
			log.Fatalf("Failed to provide DB manager: %v", err)
		}
		// Register database drivers
		manager.RegisterDriver(constants.DatabaseTypePostgreSQL, dbmanager.NewPostgresDriver())
		manager.RegisterDriver(constants.DatabaseTypeYugabyteDB, dbmanager.NewPostgresDriver()) // Use same driver for both
		manager.RegisterDriver(constants.DatabaseTypeMySQL, dbmanager.NewMySQLDriver())
		manager.RegisterDriver(constants.DatabaseTypeClickhouse, dbmanager.NewClickHouseDriver())
		manager.RegisterDriver(constants.DatabaseTypeMongoDB, dbmanager.NewMongoDBDriver())
		return manager, nil
	}); err != nil {
		log.Fatalf("Failed to provide DB manager: %v", err)
	}

	if err := DiContainer.Provide(func(db *mongodb.MongoDBClient) repositories.UserRepository {
		return repositories.NewUserRepository(db)
	}); err != nil {
		log.Fatalf("Failed to provide user repository: %v", err)
	}

	if err := DiContainer.Provide(func() repositories.TokenRepository { return tokenRepo }); err != nil {
		log.Fatalf("Failed to provide token repository: %v", err)
	}

	// Provide services
	if err := DiContainer.Provide(func(userRepo repositories.UserRepository, tokenRepo repositories.TokenRepository, jwt utils.JWTService) services.AuthService {
		return services.NewAuthService(userRepo, jwt, tokenRepo)
	}); err != nil {
		log.Fatalf("Failed to provide auth service: %v", err)
	}

	// Add LLM Manager
	if err := DiContainer.Provide(func() *llm.Manager {
		manager := llm.NewManager()

		switch config.Env.DefaultLLMClient {
		case constants.OpenAI:
			// Register default OpenAI client
			err := manager.RegisterClient(constants.OpenAI, llm.Config{
				Provider:            constants.OpenAI,
				Model:               config.Env.OpenAIModel,
				APIKey:              config.Env.OpenAIAPIKey,
				MaxCompletionTokens: config.Env.OpenAIMaxCompletionTokens,
				Temperature:         config.Env.OpenAITemperature,
				DBConfigs: []llm.LLMDBConfig{
					{
						DBType:       constants.DatabaseTypePostgreSQL,
						Schema:       constants.GetLLMResponseSchema(constants.OpenAI, constants.DatabaseTypePostgreSQL),
						SystemPrompt: constants.GetSystemPrompt(constants.OpenAI, constants.DatabaseTypePostgreSQL),
					},
					{
						DBType:       constants.DatabaseTypeYugabyteDB,
						Schema:       constants.GetLLMResponseSchema(constants.OpenAI, constants.DatabaseTypeYugabyteDB),
						SystemPrompt: constants.GetSystemPrompt(constants.OpenAI, constants.DatabaseTypeYugabyteDB),
					},
					{
						DBType:       constants.DatabaseTypeMySQL,
						Schema:       constants.GetLLMResponseSchema(constants.OpenAI, constants.DatabaseTypeMySQL),
						SystemPrompt: constants.GetSystemPrompt(constants.OpenAI, constants.DatabaseTypeMySQL),
					},
					{
						DBType:       constants.DatabaseTypeClickhouse,
						Schema:       constants.GetLLMResponseSchema(constants.OpenAI, constants.DatabaseTypeClickhouse),
						SystemPrompt: constants.GetSystemPrompt(constants.OpenAI, constants.DatabaseTypeClickhouse),
					},
					{
						DBType:       constants.DatabaseTypeMongoDB,
						Schema:       constants.GetLLMResponseSchema(constants.OpenAI, constants.DatabaseTypeMongoDB),
						SystemPrompt: constants.GetSystemPrompt(constants.OpenAI, constants.DatabaseTypeMongoDB),
					},
				},
			})
			if err != nil {
				log.Printf("Warning: Failed to register OpenAI client: %v", err)
			}
		case constants.Gemini:
			// Register default Gemini client
			err := manager.RegisterClient(constants.Gemini, llm.Config{
				Provider:            constants.Gemini,
				Model:               config.Env.GeminiModel,
				APIKey:              config.Env.GeminiAPIKey,
				MaxCompletionTokens: config.Env.GeminiMaxCompletionTokens,
				Temperature:         config.Env.GeminiTemperature,
				DBConfigs: []llm.LLMDBConfig{
					{
						DBType:       constants.DatabaseTypePostgreSQL,
						Schema:       constants.GetLLMResponseSchema(constants.Gemini, constants.DatabaseTypePostgreSQL),
						SystemPrompt: constants.GetSystemPrompt(constants.Gemini, constants.DatabaseTypePostgreSQL),
					},
					{
						DBType:       constants.DatabaseTypeYugabyteDB,
						Schema:       constants.GetLLMResponseSchema(constants.Gemini, constants.DatabaseTypeYugabyteDB),
						SystemPrompt: constants.GetSystemPrompt(constants.Gemini, constants.DatabaseTypeYugabyteDB),
					},
					{
						DBType:       constants.DatabaseTypeMySQL,
						Schema:       constants.GetLLMResponseSchema(constants.Gemini, constants.DatabaseTypeMySQL),
						SystemPrompt: constants.GetSystemPrompt(constants.Gemini, constants.DatabaseTypeMySQL),
					},
					{
						DBType:       constants.DatabaseTypeClickhouse,
						Schema:       constants.GetLLMResponseSchema(constants.Gemini, constants.DatabaseTypeClickhouse),
						SystemPrompt: constants.GetSystemPrompt(constants.Gemini, constants.DatabaseTypeClickhouse),
					},
					{
						DBType:       constants.DatabaseTypeMongoDB,
						Schema:       constants.GetLLMResponseSchema(constants.Gemini, constants.DatabaseTypeMongoDB),
						SystemPrompt: constants.GetSystemPrompt(constants.Gemini, constants.DatabaseTypeMongoDB),
					},
				},
			})
			if err != nil {
				log.Printf("Warning: Failed to register Gemini client: %v", err)
			}
		}
		return manager
	}); err != nil {
		log.Fatalf("Failed to provide LLM manager: %v", err)
	}

	// Update Chat Service provider to include DB manager setup
	if err := DiContainer.Provide(func(
		chatRepo repositories.ChatRepository,
		llmRepo repositories.LLMMessageRepository,
		dbManager *dbmanager.Manager,
		llmManager *llm.Manager,
	) services.ChatService {
		// Get default LLM client
		llmClient, err := llmManager.GetClient(config.Env.DefaultLLMClient)
		if err != nil {
			log.Printf("Warning: Failed to get default LLM client: %v", err)
		}

		chatService := services.NewChatService(chatRepo, llmRepo, dbManager, llmClient)

		// Set chat service as stream handler for DB manager
		dbManager.SetStreamHandler(chatService)

		// Set chat service in auth service
		err = DiContainer.Invoke(func(authService services.AuthService) {
			authService.SetChatService(chatService)
		})
		if err != nil {
			log.Fatalf("Failed to set chat service in auth service: %v", err)
		}
		return chatService
	}); err != nil {
		log.Fatalf("Failed to provide chat service: %v", err)
	}

	// Provide handlers
	if err := DiContainer.Provide(func(authService services.AuthService) *handlers.AuthHandler {
		return handlers.NewAuthHandler(authService)
	}); err != nil {
		log.Fatalf("Failed to provide auth handler: %v", err)
	}

	// Chat Handler
	if err := DiContainer.Provide(func(
		chatService services.ChatService,
	) *handlers.ChatHandler {
		handler := handlers.NewChatHandler(chatService)
		chatService.SetStreamHandler(handler)
		return handler
	}); err != nil {
		log.Fatalf("Failed to provide chat handler: %v", err)
	}
}

// GetAuthHandler retrieves the AuthHandler from the DI container
func GetAuthHandler() (*handlers.AuthHandler, error) {
	var handler *handlers.AuthHandler
	err := DiContainer.Invoke(func(h *handlers.AuthHandler) {
		handler = h
	})
	if err != nil {
		return nil, err
	}
	return handler, nil
}

// GetChatHandler retrieves the ChatHandler from the DI container
func GetChatHandler() (*handlers.ChatHandler, error) {
	var handler *handlers.ChatHandler
	err := DiContainer.Invoke(func(h *handlers.ChatHandler) {
		handler = h
	})
	if err != nil {
		return nil, err
	}
	return handler, nil
}
